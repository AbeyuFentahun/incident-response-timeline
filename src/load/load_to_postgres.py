# Run this script in the terminal using: python3 -m src.load.load_to_postgres
# LOCAL DEVELOPMENT FALLBACK ONLY
# Loads JSON files from data/raw/ → raw.security_logs
# NOT used in production or Airflow
import os
import re
import json
from datetime import datetime
from dotenv import load_dotenv
from src.utils.db_connection import get_connection
from src.utils.logger import get_logger

# Load environment variables into OS memory
load_dotenv("config/.env")

# Access environment variables
DATA_DIR = os.getenv("DATA_DIR")

# Initialize logger
logger = get_logger(__name__)

# Dynamically resolve base directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Directories
valid_directory_path = os.path.join(BASE_DIR, DATA_DIR, "raw")
invalid_directory_path = os.path.join(BASE_DIR, DATA_DIR, "dead_letter")

# Regex pattern to match timestamps in filenames (YYYYMMDD_HHMMSS)
pattern = r"\d{8}_\d{6}"

# SQL query
INSERT_QUERY = """
INSERT INTO raw.security_logs (
    event_id,
    event_time,
    source,
    severity,
    message,
    raw_payload,
    ingested_at
)
VALUES (
    %(event_id)s,
    %(event_time)s,
    %(source)s,
    %(severity)s,
    %(message)s,
    %(raw_payload)s,
    CURRENT_TIMESTAMP
)
ON CONFLICT (event_id) DO NOTHING;
"""


# Helper: returns sorted list of timestamped files (newest → oldest)
def get_timestamped_files(directory_path):
    try:
        file_list = [
            file for file in os.listdir(directory_path) if re.search(pattern, file)
        ]
    except Exception as e:
        logger.error(f"Could not list files in {directory_path}: {e}")
        return []

    # Makes sure the list isn't empty
    if not file_list:
        logger.error(f"No timestamped files found in {directory_path}")
        return []

    sorted_files = sorted(
        file_list,
        key=lambda f: datetime.strptime(re.search(pattern, f).group(), "%Y%m%d_%H%M%S"),
        reverse=True,
    )

    return sorted_files


def load_json_to_postgres():

    conn = None
    cursor = None

    try:
        conn = get_connection()  # DB connection (SSL enabled if configured)
        cursor = conn.cursor()

        # Get fresh file lists at runtime
        valid_files = get_timestamped_files(valid_directory_path)
        invalid_files = get_timestamped_files(invalid_directory_path)

        # Safe guard against empty valid files list
        if not valid_files:
            logger.error("No raw files available to load into PostgreSQL.")
            raise ValueError("No raw files found — load aborted.")

        # VALID FILE PROCESSING
        for file in valid_files:
            valid_file_path = os.path.join(valid_directory_path, file)

            try:
                # Read the JSON file
                with open(valid_file_path, "r", encoding="utf-8") as f:
                    valid_data = json.load(f)

                # Must be list because extract step always writes lists
                if not isinstance(valid_data, list):
                    logger.error(
                        f"Expected list of records in {valid_file_path}, but got {type(valid_data)}. Skipping."
                    )
                    continue

                logger.info(
                    f"Valid file loaded: {valid_file_path} — Records found: {len(valid_data)}"
                )

                # Fail fast: valid_data should never be empty
                if len(valid_data) == 0:
                    logger.error(
                        f"No valid records found in {valid_file_path}. Failing fast."
                    )
                    raise ValueError("Zero valid records — load step aborted.")

                # Track file-level insertion statistics
                insert_count = 0
                skip_count = 0  # duplicates
                error_count = 0

                # Process records inside this file
                for record in valid_data:
                    try:
                        # TIMESTAMP NORMALIZATION
                        # Extract timestamp (ISO-8601 from EXTRACT step)
                        ts = record["timestamp"]

                        # If it ends with 'Z', convert to +00:00 offset format
                        # Postgres handles this more consistently
                        if ts.endswith("Z"):
                            ts = ts[:-1] + "+00:00"

                        # Convert normalized string → datetime object
                        # This ensures Postgres always gets a valid timestamp
                        event_time = datetime.fromisoformat(ts)
                        # -----------------------------------------

                        # Insert into DB
                        cursor.execute(
                            INSERT_QUERY,
                            {
                                "event_id": record["event_id"],
                                "event_time": event_time,  # Use normalized timestamp
                                "source": record["source_ip"],
                                "severity": record["severity"],
                                "message": record["description"],
                                # raw_payload must be JSON, NOT a Python dict.
                                # json.dumps() converts Python dict → JSON string,
                                # which Postgres can accept as JSON/JSONB.
                                "raw_payload": json.dumps(record),
                            },
                        )

                        # ON CONFLICT DO NOTHING makes duplicates invisible to rowcount
                        if cursor.rowcount == 0:
                            skip_count += 1
                        else:
                            insert_count += 1

                    except Exception as e:
                        logger.error(f"Record-level error in {valid_file_path}: {e}")
                        error_count += 1
                        break  # Stop processing this file

                if error_count == 0:
                    # Only commit if file succeeded
                    conn.commit()
                    logger.info(f"Committed file successfully: {valid_file_path}")
                else:
                    # Roll back everything from this file (file-level atomicity)
                    conn.rollback()
                    logger.error(
                        f"File {valid_file_path} rolled back due to {error_count} record-level errors."
                    )
                    continue  # Skip to next file

                # Summary log for each file
                logger.info(
                    f"Load summary for {valid_file_path} → "
                    f"Inserted: {insert_count}, "
                    f"Skipped (duplicates): {skip_count}, "
                    f"Errors: {error_count}"
                )

            except Exception as e:
                logger.error(f"Error processing file {valid_file_path}: {e}")
                conn.rollback()
                raise  # raise error to Airflow

        # INVALID FILE PROCESSING — dead_letter
        for file in invalid_files:
            invalid_file_path = os.path.join(invalid_directory_path, file)

            try:
                with open(invalid_file_path, "r", encoding="utf-8") as f:
                    invalid_data = json.load(f)

                if isinstance(invalid_data, list):
                    logger.warning(
                        f"Invalid file processed (dead_letter): {invalid_file_path} "
                        f"Invalid records: {len(invalid_data)}"
                    )
                else:
                    logger.warning(
                        f"Invalid file {invalid_file_path} does not contain a list. Possible extract error."
                    )

            except Exception as e:
                logger.error(f"Error reading invalid file {invalid_file_path}: {e}")
                continue

    except Exception as e:
        logger.error(f"Unexpected failure during load step: {e}")
        raise

    finally:
        # Always executed, even if an exception is raised
        if cursor is not None:
            cursor.close()
            logger.info("PostgreSQL cursor closed.")

        if conn is not None:
            conn.close()
            logger.info("PostgreSQL connection closed.")


# Smoke test to see if everything loads to PostgreSQL DB
if __name__ == "__main__":
    try:
        load_json_to_postgres()
        logger.info("Load JSON to Postgres Successful!")
    except Exception as e:
        logger.error(f"Error occured when loading into Postgres: {e}.")
