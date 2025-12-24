# Run this script in terminal: python3 -m src.load.from_s3_to_postgres 
# This script extracts data from s3 respective bucket and loads it into the respective postgres table
import os
import json
from datetime import datetime, timezone
from json import JSONDecodeError
from dotenv import load_dotenv
from src.utils.logger import get_logger
from src.transform.schema_definitions import build_raw_security_log
from src.utils.aws_client import get_s3_client, test_s3_connection
from src.utils.db_connection import get_connection
from src.sql.sql_queries import RAW_INSERT_QUERY, INGESTION_LOG_INSERT
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError

# Load environment variables from .env file into memory
load_dotenv("config/.env")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX_RAW = os.getenv("S3_PREFIX_RAW")
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Validate critical env vars early so we fail fast
if not S3_BUCKET:
    raise EnvironmentError("Missing required env var: S3_BUCKET")
if not S3_PREFIX_RAW:
    raise EnvironmentError("Missing required env var: S3_PREFIX_RAW")

# Initialize logger
logger = get_logger(__name__)




# Extract data from s3
def extract_raw_events_from_s3(s3_prefix, batch_id):

    full_prefix = f"{s3_prefix.rstrip('/')}/{batch_id}/"

    logger.info(
        "Extracting raw batch from S3",
        extra={"batch_id": batch_id, "prefix": full_prefix},
    )

    try:
        s3 = get_s3_client()
        test_s3_connection()
        logger.info("S3 client initialized and connection successful.")
    except (NoCredentialsError, EndpointConnectionError, ClientError) as e:
        logger.error(f"AWS connection error: {e}")
        raise

    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=full_prefix)
    contents = response.get("Contents")

    if not contents:
        raise ValueError(f"No data files found under prefix '{full_prefix}'")

    logger.info(f"Found {len(contents)} files under prefix '{full_prefix}'")

    # Minimum file size to deal with ghost files
    MIN_FILE_SIZE_BYTES = 5 * 1024
    # Make sure files pass the minimum size
    files = [obj for obj in contents if obj.get("Size", 0) > MIN_FILE_SIZE_BYTES]
    # To see if there ghost files in our bucket
    ghost_files = [obj for obj in contents if obj.get("Size", 0) == 0]

    if ghost_files:
        logger.warning(f"Ghost files skipped: {[f['Key'] for f in ghost_files]}")

    if not files:
        raise ValueError(f"No usable files found for batch {batch_id}")

    all_events = []
    s3_keys = []
    batch_ts = None

    for obj in files:
        # Grab the key for the object in the ob
        s3_key = obj["Key"]

        try:
            response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
            data = response["Body"].read().decode("utf-8")
            logger.info(f"Read {len(data)} bytes from '{s3_key}'")
        except Exception as e:
            logger.error(f"Error reading S3 object '{s3_key}': {e}")
            raise

        if not data.strip():
            raise ValueError(f"S3 file {s3_key} is empty — cannot parse")

        try:
            data_obj = json.loads(data)

            if "events" not in data_obj or not isinstance(data_obj["events"], list):
                raise ValueError(f"'events' key missing or invalid format in {s3_key}")

        except (JSONDecodeError, UnicodeDecodeError, TypeError, ValueError) as e:
            logger.error(f"Failed to parse/validate JSON from '{s3_key}': {e}")
            raise
        
        # Make sure that the batch_id matches up with the current object in the respective s3 bucket
        payload_batch_id = data_obj.get("batch_id")
        if payload_batch_id != batch_id:
            raise ValueError(
                f"Batch mismatch in {s3_key}: expected {batch_id}, found {payload_batch_id}"
            )

        # Make sure we have the batch timestamp
        if not batch_ts:
            batch_ts = data_obj.get("batch_ts")

        # .extend() iterates over its argument and adds each element individually
        # .extend() is used to loop through the list of objects and add each object to the list individually as it own element
        # .append() would add the whole list as a single element in the list
        all_events.extend(data_obj["events"])
        s3_keys.append(s3_key)

    return batch_id, batch_ts, all_events, s3_keys




# Load extracted data from s3 to postgres 
def load_events_to_postgres(events, batch_id, conn, insert_query, record_builder):
    try:
        with conn.cursor() as cursor:
            # Prepare list of records to insert
            # Creates of list of dicts that mapped according to the schema of the table
            records = [record_builder(event, batch_id) for event in events]

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
def log_ingestion_metadata(conn, batch_id, stage, s3_key, events, status="SUCCESS", error_message=None):


    source_name = "security_event_api"  # or however you're identifying the source
    file_name = s3_key
    record_count = len(events)
    started_at = datetime.now(timezone.utc)
    finished_at = datetime.now(timezone.utc)

    log_entry = {
        "batch_id": batch_id,
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
    s3_keys = []          
    events = []
    with open(os.path.join(BASE_DIR, "latest_batch_id.txt")) as f:
        batch_id = f.read().strip()





    try:
        logger.info("Starting S3 → Postgres pipeline")

        conn = get_connection()
        batch_id, batch_ts, events, s3_keys = extract_raw_events_from_s3(S3_PREFIX_RAW, batch_id)


        load_events_to_postgres(events, batch_id, conn, RAW_INSERT_QUERY, build_raw_security_log)

        # SUCCESS log
        log_ingestion_metadata(
            conn,
            batch_id,
            stage="raw",
            s3_key=",".join(s3_keys),
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
                    batch_id=batch_id,
                    stage="raw",
                    s3_key=",".join(s3_keys) if s3_keys else "UNKNOWN",  
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
