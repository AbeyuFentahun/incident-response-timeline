# Run this script in terminal: python3 -m src.transform.transform_security_events
import os
from dotenv import load_dotenv
import pandas as pd
import json
from datetime import datetime, timezone
from src.utils.db_connection import get_connection
from src.utils.aws_client import test_s3_connection, get_s3_client
from src.utils.logger import get_logger
from src.validation.validation_raw_events import validate_raw_event

logger = get_logger(__name__)

# Load env vars into OS memory and access them
load_dotenv("config/.env")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX_RAW = os.getenv("S3_PREFIX_RAW")


# 1. Identify where you are pulling from
LATEST_BATCH_QUERY = """
SELECT 
    event_id,
    event_time,
    source_ip,
    destination_ip,
    event_type,
    severity,
    message,
    raw_payload,
    ingested_at
FROM raw.security_logs
WHERE ingested_at = (
    SELECT MAX(ingested_at) FROM raw.security_logs
);
"""

# 2. INSERT INTO parsed_events
PARSED_INSERT_QUERY = """
INSERT INTO staging.parsed_events (
    event_id,
    event_time,
    source_ip,
    destination_ip,
    event_type,
    severity_level,
    category,
    normalized_message,
    processed_at
) VALUES (
    %(event_id)s,
    %(event_time)s,
    %(source_ip)s,
    %(destination_ip)s,
    %(event_type)s,
    %(severity_level)s,
    %(category)s,
    %(normalized_message)s,
    %(processed_at)s
);
"""

# 3. INSERT INTO validation_errors
VALIDATION_ERROR_QUERY = """
INSERT INTO staging.validation_errors (
    event_id,
    event_time,
    source_ip,
    destination_ip,
    raw_event,
    error_type,
    error_message,
    logged_at
) VALUES (
    %(event_id)s,
    %(event_time)s,
    %(source_ip)s,
    %(destination_ip)s,
    %(raw_event)s,
    %(error_type)s,
    %(error_message)s,
    %(logged_at)s
);
"""

def load_raw_events_from_s3():
    # Fail fast
    if not S3_BUCKET:
        raise EnvironmentError("S3_BUCKET is not set")

    if not S3_PREFIX_RAW:
        raise ValueError("s3_prefix_raw must be provided")


    # Initialize S3 conneciton and test it
    try:
        s3 = get_s3_client()
        test_s3_connection()
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
        raise


    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX_RAW)
    except:
        logger.error("Failed to list S3 objects", exc_info=True)
        raise

    contents = response.get("Contents")

    if not contents:
        logger.error("No files found in S3 under the given prefix.")
        raise ValueError(f"No data files found under prefix '{S3_PREFIX_RAW}' — aborting.")

    
    
    


    


def transform_and_load_security_events():
    conn = None
    # Lists to track results
    valid_events = []
    invalid_events = []

    try:
        # Get connection
        conn = get_connection()
        cursor = conn.cursor()
        logger.info("Connected to database.")

        # Load latest file into memory
        df = pd.read_sql(LATEST_BATCH_QUERY, conn)


        if df.empty:
            logger.error("DataFrame is empty. No records found in latest raw batch.")
            raise ValueError("No records found in latest raw batch — aborting transform.")

        logger.info(f"Retrieved {len(df)} records from latest raw batch.")

        # Standardize, Clean, Enrich, Validate
        for _, row in df.iterrows():
            event_dict = row.to_dict()

            try:
                validated = validate_raw_event(event_dict)
                valid_events.append(validated)

                # Load valid events into PARSED EVENTS
                cursor.execute(PARSED_INSERT_QUERY, validated)

            except Exception as e:
                invalid_record = {
                    "event_id": event_dict.get("event_id"),
                    "event_time": event_dict.get("event_time"),
                    "source_ip": event_dict.get("source_ip"),
                    "destination_ip": event_dict.get("destination_ip"),
                    "raw_event": json.dumps(event_dict, default=str),
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "logged_at": datetime.now(timezone.utc)
                }

                invalid_events.append(invalid_record)

                # Insert into validation_errors table
                cursor.execute(VALIDATION_ERROR_QUERY, invalid_record)

        conn.commit()

        logger.info(f"{len(valid_events)} valid events inserted into staging.parsed_events.")
        logger.info(f"{len(invalid_events)} invalid events inserted into staging.validation_errors.")

    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
        if conn:
            conn.rollback()
            raise

    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")



if __name__ == "__main__":
    try:
        logger.info("Starting smoke test for transform_security_events")

        # Run transformation logic
        transform_and_load_security_events()

        # Basic row count check (optional, comment out if unnecessary)
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM staging.parsed_events;")
                parsed_count = cur.fetchone()[0]

                cur.execute("SELECT COUNT(*) FROM staging.validation_errors;")
                error_count = cur.fetchone()[0]

        logger.info(f"Parsed events written: {parsed_count}")
        logger.info(f"Validation errors written: {error_count}")
        logger.info("Smoke test completed successfully.")

    except Exception as e:
        logger.exception("Smoke test failed due to unexpected error")
        raise





