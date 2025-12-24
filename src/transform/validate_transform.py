'''
Does the event have all transformed fields? done
Are severity_level and category present and consistent?
Is processed_at set and UTC?
Is normalized_message non-empty?
Did transform accidentally drop required canonical fields?
'''
from datetime import datetime, timezone

def validate_transformation(data: dict):
    REQUIRED_TRANSFORM_FIELDS = [
        "severity_level",
        "category",
        "normalized_message",
        "processed_at",
    ]
    
    REQUIRED_CANONICAL_FIELDS = [
    "event_id",
    "event_time",
    "source_ip",
    "destination_ip",
    "event_type",
    "severity",
    ]

    missing_canonical = [field for field in REQUIRED_CANONICAL_FIELDS if field not in data or data[field] is None]

    if missing_canonical:
        raise ValueError(f"Canonical field(s) missing after transform: {missing_canonical}")

    missing = [field for field in REQUIRED_TRANSFORM_FIELDS if field not in data or data[field] is None]

    if missing:
        raise ValueError(f"Missing transformed field(s) during post-transform validation: {missing}")

    # Consistency checks
    if data["severity_level"] != data["severity"]:
        raise ValueError("severity_level does not match severity")

    if data["category"] != data["event_type"]:
        raise ValueError("category does not match event_type")

    if not data["normalized_message"].strip():
        raise ValueError("normalized_message is empty")
    
    processed_at = data["processed_at"]

    if isinstance(processed_at, str):
        try:
            processed_at = datetime.fromisoformat(processed_at)
        except ValueError:
            raise ValueError("processed_at is not a valid ISO datetime")

    if not isinstance(processed_at, datetime):
        raise ValueError("processed_at must be a datetime")

    if processed_at.tzinfo is None or processed_at.tzinfo != timezone.utc:
        raise ValueError("processed_at must be timezone-aware and UTC")


    

