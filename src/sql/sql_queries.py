"""
Postgres SQL query definitions for the Incident Response Pipeline.

These queries define schema contracts and are imported by pipeline
modules that perform inserts and logging.
"""

# SQL insert query for raw.security_logs table
RAW_INSERT_QUERY = """
INSERT INTO raw.security_logs (
    batch_id,
    event_id,
    event_time,
    source_ip,
    destination_ip,
    event_type,
    severity,
    message,
    raw_payload,
    ingested_at
)
VALUES (
    %(batch_id)s,
    %(event_id)s,
    %(event_time)s,
    %(source_ip)s,
    %(destination_ip)s,
    %(event_type)s,
    %(severity)s,
    %(message)s,
    %(raw_payload)s,
    CURRENT_TIMESTAMP
)
ON CONFLICT (event_id) DO NOTHING;
"""



# SQL insert query for raw.ingestion_log table
INGESTION_LOG_INSERT = """
INSERT INTO raw.ingestion_log (
    batch_id,
    stage,
    source_name,
    s3_key,
    record_count,
    status,
    error_message,
    started_at,
    finished_at
)
VALUES (
    %(batch_id)s,
    %(stage)s,
    %(source_name)s,
    %(s3_key)s,
    %(record_count)s,
    %(status)s,
    %(error_message)s,
    %(started_at)s,
    %(finished_at)s
)
ON CONFLICT (batch_id, stage)
DO UPDATE SET
    record_count = EXCLUDED.record_count,
    s3_key = EXCLUDED.s3_key,
    status = EXCLUDED.status,
    error_message = EXCLUDED.error_message,
    finished_at = EXCLUDED.finished_at;
"""


# SQL insert query for staging.parsed_events table
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
ON CONFLICT (event_id) DO NOTHING
"""

# SQL insert query for staging.validation_errors table
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
ON CONFLICT (event_id, error_type) DO NOTHING
"""
