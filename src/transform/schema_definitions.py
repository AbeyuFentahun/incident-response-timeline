import json
from datetime import datetime, timezone



# record builder for each table
# determines how the data will look before it is inserted into Postgres
# For raw.security_logs table
def build_raw_security_log(event: dict, batch_id: str) -> dict:
    return {
        "batch_id": batch_id,
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