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


# Validates and normalizes an incoming raw security event.
# Raises ValueError on invalid events.
# Returns normalized dict on success.
def validate_raw_event(data):
    try:
        # 1. STRUCTURE VALIDATION
        if not isinstance(data, dict):
            raise ValueError("Data must be a dictionary")

        if not data:
            raise ValueError("JSON object is empty")

        # 2. REQUIRED FIELDS VALIDATION
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise ValueError(f"Missing required fields {missing}")

        # 3. OPTIONAL FIELD NORMALIZATION
        for field in optional_string_fields:
            if field in data:
                if not isinstance(data[field], str):
                    raise ValueError(f"{field} must be a string")
                data[field] = data[field].strip().lower()
            else:
                data[field] = None  # consistent schema for downstream

        # 4. TYPE VALIDATION (REQUIRED FIELDS)
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

        if not isinstance(data["severity"], (str, int)):
            raise ValueError("severity must be a string or integer")

        if not isinstance(data["message"], str):
            raise ValueError("message must be a string")

        # 5. RAW PAYLOAD VALIDATION
        if "raw_payload" in data and data["raw_payload"] is not None:

            # Handle both dict (already parsed) and string (JSON text)
            if isinstance(data["raw_payload"], str):
                # Ensure it's valid JSON text
                try:
                    json.loads(data["raw_payload"])
                except Exception:
                    raise ValueError("raw_payload is not valid JSON")
            else:
                # Must be JSON-serializable (covers dict/object cases)
                try:
                    json.dumps(data["raw_payload"])
                except Exception:
                    raise ValueError("raw_payload contains non-serializable data")

            # Max size limit (~50KB)
            if len(str(data["raw_payload"])) > 50000:
                raise ValueError("raw_payload is too large")

        # 6. NORMALIZATION (REQUIRED FIELDS)
        data["event_id"] = str(data["event_id"]).strip()
        data["event_time"] = str(data["event_time"]).strip()
        data["source_ip"] = str(data["source_ip"]).strip()
        data["destination_ip"] = str(data["destination_ip"]).strip()
        data["event_type"] = str(data["event_type"]).strip().lower()
        data["severity"] = str(data["severity"]).strip().lower()
        data["message"] = str(data["message"]).strip()

        # 7. NON-EMPTY CHECKS
        if not data["event_id"]:
            raise ValueError("event_id does NOT exist")

        if not data["event_time"]:
            raise ValueError("event_time does NOT exist")

        if not data["source_ip"]:
            raise ValueError("source_ip does NOT exist")

        if not data["destination_ip"]:
            raise ValueError("destination_ip does NOT exist")

        if not data["event_type"]:
            raise ValueError("event_type does NOT exist")

        if not data["severity"]:
            raise ValueError("severity does NOT exist")

        if not data["message"]:
            raise ValueError("message does NOT exist")

        # ----------------------------------------------------
        # 8. FIXED TIMESTAMP FORMAT VALIDATION
        # ----------------------------------------------------
        try:
            raw_ts = str(data["event_time"]).strip()

            # Normalize common formats
            if raw_ts.endswith("Z"):
                raw_ts = raw_ts.replace("Z", "+00:00")

            # If there's no timezone at all, add UTC
            if "+" not in raw_ts and "T" in raw_ts:
                raw_ts = raw_ts + "+00:00"

            parsed_timestamp = datetime.fromisoformat(raw_ts)

        except Exception:
            raise ValueError(f"Invalid event_time format: {data['event_time']}")

        # 9. TIMESTAMP BUSINESS RULES
        parsed_timestamp = parsed_timestamp.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)

        if parsed_timestamp > now:
            raise ValueError("timestamp cannot be from the future")

        if (now - parsed_timestamp).days > 90:
            raise ValueError("timestamp is older than 90 days")

        # 10. IPv4 ADDRESS VALIDATION
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

        # 11. CATEGORY / SEVERITY VALIDATION
        if data["severity"] not in allowed_severity:
            raise ValueError("Invalid severity type")

        if data["event_type"] not in allowed_event_types:
            raise ValueError("Invalid event type")

        # 12. LENGTH CONSTRAINTS
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

        # 13. ADD NORMALIZATION TIMESTAMP
        data["normalized_at"] = datetime.now(timezone.utc).isoformat()

        # 14. ENRICHMENT FOR TRANSFORMATION PHASE
        # These fields are required by PARSED_INSERT_QUERY

        # Severity level (same as normalized severity)
        data["severity_level"] = data["severity"]

        # Category — derived from event_type
        # You can refine this later, but this will let the pipeline run
        data["category"] = data["event_type"]

        # Normalized message (strip/clean the message)
        data["normalized_message"] = data["message"].strip()

        # Timestamp when transform occurred
        data["processed_at"] = datetime.now(timezone.utc)


        # Finished
        return data

    except Exception:
        raise

