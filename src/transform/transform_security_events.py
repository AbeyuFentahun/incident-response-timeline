# Run this script in terminal: python3 -m src.transform.transform_security_events
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
from src.utils.db_connection import get_connection
from src.load.from_s3_to_postgres import extract_raw_events_from_s3
from src.transform.schema_definitions import build_raw_security_log, build_staging_parsed_event, build_validation_error_record
from src.utils.logger import get_logger
from src.transform.validate_transform import validate_transformation
from src.validation.validation_raw_events import canonicalize_event, validate_event, normalize_event
from src.transform.s3_batch_writer import transformed_batch_to_s3
from src.sql.sql_queries import PARSED_INSERT_QUERY, VALIDATION_ERROR_QUERY

logger = get_logger(__name__)

# Load env vars into OS memory and access them
load_dotenv("config/.env")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX_RAW = os.getenv("S3_PREFIX_RAW")


# Dynamical resolve file paths
# Get root directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Dynamically create path to grab the latest batch_id from the .txt file
latest_batch_path = os.path.join(BASE_DIR, "latest_batch_id.txt")

with open(os.path.join(BASE_DIR, "latest_batch_id.txt")) as f:
    batch_id = f.read().strip()


def run_transform_for_batch(batch_id: str):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    valid_s3_key = f"staging/{batch_id}/valid_events_{timestamp}.json"
    invalid_s3_key = f"dead_letter/{batch_id}/invalid_events_{timestamp}.json"

    # Extract batch from S3 
    try:
        payload_batch_id, batch_ts, all_events, s3_keys = extract_raw_events_from_s3(S3_PREFIX_RAW, batch_id)
        logger.info(
            "Starting transform for batch",
            extra={
                "batch_id": batch_id,
                "total_events": len(all_events),
                "s3_objects": len(s3_keys),
                },
            )

    except Exception as e:
        logger.error(f"S3 extraction failed for batch_id={batch_id}: {e}")
        raise

    conn = None
    cursor = None
    valid_count = 0
    invalid_count = 0
    # These list will be uploaded into respective s3 buckets
    # Store valid events
    valid_events = []
    # Store invalid events
    invalid_events = []

    try:
        # Set up DB connection
        conn = get_connection()
        cursor = conn.cursor()
        logger.info("Database connection established")

        # Per-event processing for canonicalization, validation, & normalization
        for raw_event in all_events:
            try:
                # Build canonical raw event (API → pipeline schema)
                canonical_event = build_raw_security_log(raw_event, batch_id)

                logger.info(
                    "Built canonical raw event",
                    extra={"event_id": raw_event.get("event_id"), "batch_id": batch_id},
                    )
                
                # Canonicalize values (strip, lowercase, normalize timestamp)
                canonical_event = canonicalize_event(canonical_event)

                # Validate canonical schema
                validate_event(canonical_event)

                # Normalize (adds severity_level, category, normalized_message)
                canonical_event = normalize_event(canonical_event)
                logger.info(
                    "Canonical event validated and normalized",
                    extra={"event_id": canonical_event.get("event_id")},
                    )

                # Build staging record
                parsed_event = build_staging_parsed_event(canonical_event)

                # Validate transformation happened correctly
                validate_transformation(canonical_event)

                valid_events.append(parsed_event)
                valid_count += 1

                logger.info(
                    "Event validated successfully",
                    extra={"event_id": canonical_event.get("event_id")},
                )


            except Exception as e:
                logger.warning(
            "Event failed validation",
            extra={
                "event_id": raw_event.get("event_id"),
                "error": str(e),
            },
        )
                error_record = build_validation_error_record(canonical_event, e)
                invalid_events.append(error_record)
                invalid_count += 1


                # Will implement upload to S3 dead_letter later


        # Will implement upload to S3 staging later
        transformed_batch_to_s3(valid_events, S3_BUCKET, valid_s3_key)
        transformed_batch_to_s3(invalid_events, S3_BUCKET, invalid_s3_key)

        for event in valid_events:
            cursor.execute(PARSED_INSERT_QUERY, event)

        for error_record in invalid_events:
            cursor.execute(VALIDATION_ERROR_QUERY, error_record)

        # Commit once per batch
        conn.commit()
        logger.info(
            "Transform completed",
            extra={
                "batch_id": batch_id,
                "valid_events": valid_count,
                "invalid_events": invalid_count,
                "s3_keys": s3_keys,
            },
        )

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Transform failed for batch_id={batch_id}: {e}")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    




if __name__ == "__main__":
    try:
        logger.info(f"Starting to extract from s3 raw bucket")
        run_transform_for_batch(batch_id)
        logger.info("Transformation phase completed successfully!")
    
    except Exception as e:
        logger.error(f"Unexpected Error Occured: {e}")

