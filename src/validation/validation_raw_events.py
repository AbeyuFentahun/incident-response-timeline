# Run this script in the terminal using: python3 -m src.validation.validate_raw_events
from datetime import datetime, timezone
import re
import json

# Required fields every event must have
required_fields = [
    "event_id", 
    "event_time", 
    "source_ip",
    "destination_ip", 
    "event_type", 
    "severity", 
    "message"
    ]


# Optional metadata fields
optional_string_fields = [
    "host",
    "device_id",
    "username",
    "application",
    "platform",
    "vendor",
]

# Allowed values
allowed_severity = {"low", "medium", "high", "critical"}
allowed_event_types = {
    "unauthorized_login",
    "malware_detected",
    "port_scan",
    "brute_force",
    "policy_violation",
    "dns_tunnel_detected",
    "data_exfiltration",
    "unauthorized_access",
    "phishing_click",
    "firewall_block",
}


# Canonicalize events (make sure data is able to be evalauted properly)
def canonicalize_event(data):
    # Required fields
    data["event_id"] = str(data["event_id"]).strip()
    data["event_time"] = str(data["event_time"]).strip()
    data["source_ip"] = str(data["source_ip"]).strip()
    data["destination_ip"] = str(data["destination_ip"]).strip()
    data["event_type"] = str(data["event_type"]).strip().lower()
    data["severity"] = str(data["severity"]).strip().lower()
    data["message"] = str(data["message"]).strip()

    # Optional string fields (canonicalization only)
    for field in optional_string_fields:
        if field in data and data[field] is not None:
            data[field] = str(data[field]).strip().lower()
        else:
            data[field] = None

    return data


def validate_event(data):
    # 1. STRUCTURE VALIDATION
    if not isinstance(data, dict):
        raise ValueError("Data must be a dictionary")

    if not data:
        raise ValueError("JSON object is empty")

    # 2. REQUIRED FIELDS VALIDATION
    missing = [field for field in required_fields if field not in data]
    if missing:
        raise ValueError(f"Missing required fields {missing}")

    # 3. TYPE VALIDATION
    if not isinstance(data["event_id"], str):
        raise ValueError("event_id must be a string")

    if not isinstance(data["event_time"], (str, datetime)):
        raise ValueError("event_time must be a string or datetime")

    if not isinstance(data["source_ip"], str):
        raise ValueError("source_ip must be a string")

    if not isinstance(data["destination_ip"], str):
        raise ValueError("destination_ip must be a string")

    if not isinstance(data["event_type"], str):
        raise ValueError("event_type must be a string")

    if not isinstance(data["severity"], str):
        raise ValueError("severity must be a string")

    if not isinstance(data["message"], str):
        raise ValueError("message must be a string")

    # 4. NON-EMPTY CHECKS
    for field in ["event_id", "event_time", "source_ip", "destination_ip", "event_type", "severity", "message"]:
        if not data[field]:
            raise ValueError(f"{field} does NOT exist")

    # 5. RAW PAYLOAD VALIDATION
    if "raw_payload" in data and data["raw_payload"] is not None:
        try:
            json.dumps(data["raw_payload"])
        except Exception:
            raise ValueError("raw_payload contains non-serializable data")

        if len(str(data["raw_payload"])) > 50000:
            raise ValueError("raw_payload is too large")

    # 6. TIMESTAMP VALIDATION
    raw_ts = data["event_time"]

    if isinstance(raw_ts, datetime):
        raw_ts = raw_ts.isoformat()

    if raw_ts.endswith("Z"):
        raw_ts = raw_ts.replace("Z", "+00:00")

    if "+" not in raw_ts and "T" in raw_ts:
        raw_ts = raw_ts + "+00:00"

    try:
        parsed_timestamp = datetime.fromisoformat(raw_ts)
    except Exception:
        raise ValueError(f"Invalid event_time format: {data['event_time']}")

    parsed_timestamp = parsed_timestamp.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)

    if parsed_timestamp > now:
        raise ValueError("timestamp cannot be from the future")

    if (now - parsed_timestamp).days > 90:
        raise ValueError("timestamp is older than 90 days")

    # 7. IPV4 VALIDATION
    ipv4_pattern = re.compile(
        r"^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\."
        r"(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\."
        r"(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\."
        r"(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
    )

    if not ipv4_pattern.match(data["source_ip"]):
        raise ValueError(f"Invalid source_ip format: {data['source_ip']}")

    if not ipv4_pattern.match(data["destination_ip"]):
        raise ValueError(f"Invalid destination_ip format: {data['destination_ip']}")

    # 8. DOMAIN VALIDATION
    if data["severity"] not in allowed_severity:
        raise ValueError("Invalid severity type")

    if data["event_type"] not in allowed_event_types:
        raise ValueError("Invalid event type")

    # 9. LENGTH CONSTRAINTS
    if not (1 <= len(data["event_id"]) <= 128):
        raise ValueError("event_id length must be between 1 and 128 characters")

    if not (7 <= len(data["source_ip"]) <= 15):
        raise ValueError("source_ip length invalid")

    if not (7 <= len(data["destination_ip"]) <= 15):
        raise ValueError("destination_ip length invalid")

    if not (1 <= len(data["event_type"]) <= 50):
        raise ValueError("event_type length must be between 1 and 50 characters")

    if not (1 <= len(data["severity"]) <= 10):
        raise ValueError("severity length must be between 1 and 10 characters")

    if not (1 <= len(data["message"]) <= 2000):
        raise ValueError("message length must be between 1 and 2000 characters")

    return True



# Normalize events (standardize data)
def normalize_event(data):
    now = datetime.now(timezone.utc)

    data["normalized_at"] = now.isoformat()
    data["severity_level"] = data["severity"]
    data["category"] = data["event_type"]

    # SAFE handling
    msg = data.get("message")
    data["normalized_message"] = msg.strip() if isinstance(msg, str) else ""

    data["processed_at"] = now
    return data


