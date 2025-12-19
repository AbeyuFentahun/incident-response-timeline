# Run this script in terminal: python3 -m src.load.from_s3_to_postgres 
# This script extracts data from s3 respective bucket and loads it into the respective postgres table
import os
import json
from datetime import datetime, timezone
import uuid
from json import JSONDecodeError
from dotenv import load_dotenv
from src.utils.logger import get_logger
from src.utils.aws_client import get_s3_client, test_s3_connection
from src.utils.db_connection import get_connection
from src.sql.sql_queries import RAW_INSERT_QUERY, INGESTION_LOG_INSERT
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




# record builder for each table
# determines how the data will look before it is inserted into Postgres
# For raw.security_logs table
def build_raw_security_log(event: dict) -> dict:
    return {
        "event_id": event.get("event_id"),
        "event_time": event.get("timestamp"),
        "source_ip": event.get("source_ip"),
        "destination_ip": event.get("destination_ip"),
        "event_type": event.get("event_type"),
        "severity": event.get("severity"),
        "message": event.get("description") or "No message provided",
        "raw_payload": json.dumps(event, default=str),
        # ingested_at is handled by DEFAULT / CURRENT_TIMESTAMP in SQL
    }

# For staging.parsed_events table
def build_staging_parsed_event(event: dict) -> dict:
    return {
        "event_id": event["event_id"],
        "event_time": event["event_time"],
        "source_ip": event["source_ip"],
        "destination_ip": event["destination_ip"],
        "event_type": event["event_type"],
        "severity_level": event["severity_level"],
        "category": event["category"],
        "normalized_message": event["normalized_message"],
        "processed_at": event.get("processed_at") or datetime.now(timezone.utc),
    }

# For staging.validation_errors table
def build_validation_error_record(event: dict, error: Exception) -> dict:
    return {
        "event_id": event.get("event_id"),
        "event_time": event.get("event_time"),
        "source_ip": event.get("source_ip"),
        "destination_ip": event.get("destination_ip"),
        "raw_event": json.dumps(event),
        "error_type": type(error).__name__,
        "error_message": str(error),
        "logged_at": datetime.now(timezone.utc),
    }



# Extract data from s3
def extract_raw_events_from_s3(s3_prefix):
    try:
        # Initialize and test S3 client
        s3 = get_s3_client()
        test_s3_connection()
        logger.info("S3 client initialized and connection successful.")

    except (NoCredentialsError, EndpointConnectionError, ClientError) as e:
        logger.error(f"AWS connection error: {e}")
        raise

    # List objects under raw/ prefix
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_prefix)
    contents = response.get("Contents")

    if not contents:
        logger.error("No files found in S3 under the given prefix.")
        raise ValueError(f"No data files found under prefix '{s3_prefix}' — aborting.")


    logger.info(f"Found {len(contents)} files under prefix '{s3_prefix}'.")

    # Filter out ghost files and enforce file size threshold
    MIN_FILE_SIZE_BYTES = 5 * 1024  # 5KB threshold to skip ghost/corrupt files
    files = [obj for obj in contents if obj.get("Size", 0) > MIN_FILE_SIZE_BYTES]
    ghost_files = [obj for obj in contents if obj.get("Size", 0) == 0]

    if ghost_files:
        logger.warning(f"Ghost files skipped: {[f['Key'] for f in ghost_files]}")

    if not files:
        logger.error("Only ghost files or too-small files found.")
        raise ValueError("No usable files found in S3.")

    # Grab most recent file
    latest_file = sorted(files, key=lambda x: x["LastModified"], reverse=True)[0]
    s3_key = latest_file["Key"]
    logger.info(f"Latest file: {s3_key} (LastModified: {latest_file['LastModified']})")

    try:
        # Fetch and decode contents
        obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        data = obj["Body"].read().decode("utf-8")
        logger.info(f"Read {len(data)} bytes from '{s3_key}'")

    except Exception as e:
        logger.error(f"Error reading S3 object '{s3_key}': {e}")
        raise

    if not data.strip():
        raise ValueError(f"S3 file {s3_key} is empty — cannot parse")

    try:
        # Parse and validate top-level JSON structure
        data_obj = json.loads(data)
        if "events" not in data_obj or not isinstance(data_obj["events"], list):
            raise ValueError(f"'events' key missing or invalid format in JSON file: {s3_key}")

        logger.info(f"Parsed {len(data_obj['events'])} events from '{s3_key}'")

    except (JSONDecodeError, UnicodeDecodeError, TypeError, ValueError) as e:
        logger.error(f"Failed to parse/validate JSON from '{s3_key}': {e}")
        raise

    return data_obj["events"], s3_key


# Load extracted data from s3 to postgres 
def load_events_to_postgres(events, conn, insert_query, record_builder):
    try:
        with conn.cursor() as cursor:
            # Prepare list of records to insert
            # Creates of list of dicts that mapped according to the schema of the table
            records = [record_builder(event) for event in events]

            if not records:
                logger.warning("No records to insert into Postgres.")
                return


            # Insert into database
            cursor.executemany(insert_query, records)
            conn.commit()
            logger.info(f"{len(records)} records loaded into Postgres.")


    except Exception as e:
        logger.error(f"Failed to insert records into DB: {e}")
        conn.rollback()
        raise


# Logs metadata about the current ingestion batch to raw.ingestion_log.
def log_ingestion_metadata(conn, job_id, stage, s3_key, events, status="SUCCESS", error_message=None):


    source_name = "security_event_api"  # or however you're identifying the source
    file_name = s3_key
    record_count = len(events)
    started_at = datetime.now(timezone.utc)
    finished_at = datetime.now(timezone.utc)

    log_entry = {
        "job_id": job_id,
        "stage": stage,
        "source_name": "security_event_api",
        "s3_key": s3_key,
        "record_count": len(events),
        "status": status.upper(),
        "error_message": error_message,
        "started_at": datetime.now(timezone.utc),
        "finished_at": datetime.now(timezone.utc),
    }

    try:
        with conn.cursor() as cursor:
            cursor.execute(INGESTION_LOG_INSERT, log_entry)
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to insert ingestion log: {e}")
        conn.rollback()
        raise



# Smoke test to run standalone extract-load
if __name__ == "__main__":
    conn = None
    s3_key = None
    events = []
    job_id = str(uuid.uuid4())



    try:
        logger.info("Starting S3 → Postgres pipeline")

        conn = get_connection()
        events, s3_key = extract_raw_events_from_s3(S3_PREFIX_RAW)

        load_events_to_postgres(events, conn, RAW_INSERT_QUERY, build_raw_security_log)

        # SUCCESS log
        log_ingestion_metadata(
            conn,
            job_id,
            stage="raw",
            s3_key=s3_key,
            events=events,
            status="SUCCESS"
        )

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")


        # FAILURE log
        if conn:
            try:
                log_ingestion_metadata(
                    conn,
                    job_id=job_id,
                    stage="raw",
                    s3_key=s3_key or "UNKNOWN",
                    events=events or [],
                    status="FAILED",
                    error_message=str(e),
                )
            except Exception as log_err:
                logger.error(f"Failed to write FAILED ingestion_log entry: {log_err}")


        raise  # re-raise exception so Airflow sees failure

    finally:
        if conn:
            try:
                conn.close()
                logger.info("Database connection closed.")
            except Exception as close_err:
                logger.error(f"Failed to close DB connection: {close_err}")
