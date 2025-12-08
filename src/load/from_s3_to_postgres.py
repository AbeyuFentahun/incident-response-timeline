# Run this script in terminal: python3 -m src.load.from_s3_to_postgres 
import os
import json
from datetime import datetime, timezone
import uuid
from json import JSONDecodeError
from dotenv import load_dotenv
from src.utils.logger import get_logger
from src.utils.aws_client import get_s3_client, test_s3_connection
from src.utils.db_connection import get_connection
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError

# Load environment variables from .env file into memory
load_dotenv("config/.env")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX_RAW = os.getenv("S3_PREFIX_RAW")

# Validate critical env vars early so we fail fast
if not S3_BUCKET:
    raise EnvironmentError("Missing required env var: S3_BUCKET")
if not S3_PREFIX_RAW:
    raise EnvironmentError("Missing required env var: S3_PREFIX_RAW")

# Initialize logger
logger = get_logger(__name__)

# SQL insert query for raw.security_logs table
RAW_INSERT_QUERY = """
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

INGESTION_LOG_INSERT = """
INSERT INTO raw.ingestion_log (
    job_id,
    source_name,
    file_name,
    record_count,
    status,
    error_message,
    started_at,
    finished_at
)
VALUES (
    %(job_id)s,
    %(source_name)s,
    %(file_name)s,
    %(record_count)s,
    %(status)s,
    %(error_message)s,
    %(started_at)s,
    %(finished_at)s
);
"""

# Extract data from s3
def s3_to_postgres():
    try:
        # Step 1: Initialize and test S3 client
        s3 = get_s3_client()
        test_s3_connection()
        logger.info("S3 client initialized and connection successful.")

    except (NoCredentialsError, EndpointConnectionError, ClientError) as e:
        logger.error(f"AWS connection error: {e}")
        raise

    # Step 2: List objects under raw/ prefix
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX_RAW)
    contents = response.get("Contents")

    if not contents:
        logger.error("No files found in S3 under the given prefix.")
        raise ValueError("No raw data files found — aborting.")

    logger.info(f"Found {len(contents)} files under prefix '{S3_PREFIX_RAW}'.")

    # Step 3: Filter out ghost files and enforce file size threshold
    MIN_FILE_SIZE_BYTES = 5 * 1024  # 5KB threshold to skip ghost/corrupt files
    files = [obj for obj in contents if obj.get("Size", 0) > MIN_FILE_SIZE_BYTES]
    ghost_files = [obj for obj in contents if obj.get("Size", 0) == 0]

    if ghost_files:
        logger.warning(f"Ghost files skipped: {[f['Key'] for f in ghost_files]}")

    if not files:
        logger.error("Only ghost files or too-small files found.")
        raise ValueError("No usable files found in S3.")

    # Step 4: Grab most recent file
    latest_file = sorted(files, key=lambda x: x["LastModified"], reverse=True)[0]
    s3_key = latest_file["Key"]
    logger.info(f"Latest file: {s3_key} (LastModified: {latest_file['LastModified']})")

    try:
        # Step 5: Fetch and decode contents
        obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        data = obj["Body"].read().decode("utf-8")
        logger.info(f"Read {len(data)} bytes from '{s3_key}'")

    except Exception as e:
        logger.error(f"Error reading S3 object '{s3_key}': {e}")
        raise

    if not data.strip():
        raise ValueError(f"S3 file {s3_key} is empty — cannot parse")

    try:
        # Step 6: Parse and validate top-level JSON structure
        data_obj = json.loads(data)
        if "events" not in data_obj or not isinstance(data_obj["events"], list):
            raise ValueError(f"'events' key missing or invalid format in JSON file: {s3_key}")

        logger.info(f"Parsed {len(data_obj['events'])} events from '{s3_key}'")

    except (JSONDecodeError, UnicodeDecodeError, TypeError, ValueError) as e:
        logger.error(f"Failed to parse/validate JSON from '{s3_key}': {e}")
        raise

    return data_obj["events"], s3_key


# Load extracted data from s3 to postgres raw.security_logs
def load_raw_events(events, conn):
    try:
        with conn.cursor() as cursor:
            # Step 7: Prepare list of records to insert
            records = []
            for event in events:
                record = {
                    "event_id": event.get("event_id"),
                    "event_time": event.get("timestamp"),
                    "source": event.get("source_ip"),  # Corrected key
                    "severity": event.get("severity"),
                    "message": event.get("message", "No message provided"),
                    "raw_payload": json.dumps(event)
                }
                records.append(record)

            # Step 8: Insert into database
            cursor.executemany(RAW_INSERT_QUERY, records)
            conn.commit()
            logger.info(f"{len(records)} events loaded into raw.security_logs.")

    except Exception as e:
        logger.error(f"Failed to insert records into DB: {e}")
        conn.rollback()
        raise


# Logs metadata about the current ingestion batch to raw.ingestion_log.
def log_ingestion_metadata(conn, s3_key, events, status="success", error_message=None):


    job_id = str(uuid.uuid4())
    source_name = "security_event_api"  # or however you're identifying the source
    file_name = s3_key
    record_count = len(events)
    started_at = datetime.now(timezone.utc)
    finished_at = datetime.now(timezone.utc)

    log_entry = {
        "job_id": job_id,
        "source_name": source_name,
        "file_name": file_name,
        "record_count": record_count,
        "status": status.upper(),
        "error_message": error_message,
        "started_at": started_at,
        "finished_at": finished_at
    }

    try:
        with conn.cursor() as cursor:
            cursor.execute(INGESTION_LOG_INSERT, log_entry)
            conn.commit()
    except Exception as e:
        logger.error(f"❌ Failed to insert ingestion log: {e}")
        conn.rollback()
        raise



# Smoke test to run standalone extract-load
if __name__ == "__main__":
    conn = None
    s3_key = None
    events = []

    try:
        logger.info("Starting S3 → Postgres pipeline")

        # Connect to DB
        conn = get_connection()

        # Extract & fetch file from S3
        events, s3_key = s3_to_postgres()

        # Load into raw.security_logs
        load_raw_events(events, conn)
        logger.info(f"Successfully loaded events from {s3_key}")

        # Log SUCCESS
        log_ingestion_metadata(
            conn,
            s3_key=s3_key,
            events=events,
            status="SUCCESS",
            error_message=None
        )
        logger.info(f"Successfully logged ingestion metadata for {s3_key}")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")

        # Always attempt to write FAILED to ingestion_log
        try:
            if conn:
                log_ingestion_metadata(
                    conn,
                    s3_key=s3_key or "UNKNOWN",
                    events=events or [],
                    status="FAILED",
                    error_message=str(e)
                )
        except Exception as log_err:
            logger.error(f"Failed to write FAILED ingestion_log entry: {log_err}")

        raise

    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

