# Run this script in terminal: python3 src/api/mock_event_generator.py
import uuid
from datetime import datetime, timezone, timedelta # timedelta used to store time interval
import random

# =================================== VALID EVENTS GENERATOR =========================================#
# Allowed values
allowed_severity = ["low", "medium", "high", "critical"]

allowed_event_types = [
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
]


event_descriptions = {
    "unauthorized_login": "Failed SSH login attempt detected.",
    "malware_detected": "Malware signature detected by antivirus.",
    "port_scan": "Suspicious port scanning activity observed.",
    "brute_force": "Multiple failed authentication attempts triggered lockout.",
    "policy_violation": "User attempted to download restricted file.",
    "dns_tunnel_detected": "Unusual DNS query volume detected.",
    "data_exfiltration": "Outbound data transfer exceeded threshold.",
    "unauthorized_access": "Privileged access attempt from non-admin workstation.",
    "phishing_click": "Employee clicked on a phishing simulation link.",
    "firewall_block": "Inbound connection blocked by firewall rule set.",
}


# Generates unique event_id
def generate_unique_event_id():
    unique_id = uuid.uuid4().hex[:8]
    event_id = f"evt_{unique_id}"
    return event_id


# Generate timestamp for security events
def generate_timestamp():
    now = datetime.now(timezone.utc)
    # Subtract a random number of minutes to simulate realistic event timing
    delta = timedelta(minutes=random.randint(0, 59))
    timestamp = now - delta
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


# Generate a valid PUBLIC IPv4 address for source_ip
def generate_public_ip():
    # Valid first octets (1–223 but excluding 10, 127, 172, 192)
    invalid = {10, 127, 172, 192}
    first = random.choice([i for i in range(1, 224) if i not in invalid])

    return f"{first}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"


# Generate a valid PRIVATE IPv4 address for destination_ip
def generate_private_ip():
    # list of the THREE valid private IP ranges, each represented as a tuple
    private_blocks = [
        (10, random.randint(0, 255), random.randint(0, 255), random.randint(1, 254)),
        (192, 168, random.randint(0, 255), random.randint(1, 254)),
        (172, random.randint(16, 31), random.randint(0, 255), random.randint(1, 254)),
    ]

    block = random.choice(private_blocks)
    return ".".join(str(part) for part in block)


# Generate random event typpe
def generate_event_type():
    return random.choice(allowed_event_types)


# Generate random severity
def generate_random_severity():
    return random.choice(allowed_severity)


# Generate random description
def generate_random_description(event_type):
    # Return description if the event type exists, otherwise use fallback
    return event_descriptions.get(event_type, "Suspicious activity detected.")


# Generate valid security events
def generate_valid_event():
    event_type = generate_event_type()  # Generate event type

    return {
        "event_id": generate_unique_event_id(),
        "timestamp": generate_timestamp(),
        "source_ip": generate_public_ip(),
        "destination_ip": generate_private_ip(),
        "event_type": event_type,
        "severity": generate_random_severity(),
        "description": generate_random_description(event_type),  # Pass event_type
    }


# =================================== INVALID EVENTS GENERATOR =========================================#
def generate_invalid_event():
    event = generate_valid_event()

    # Chooses the type of corruption
    corruption = random.choice(
        [
            "missing_field",
            "bad_source_ip",
            "bad_destination_ip",
            "bad_timestamp",
            "bad_type",
        ]
    )

    # Missing field corruption
    if corruption == "missing_field":
        missing_field = [
            "timestamp",
            "source_ip",
            "destination_ip",
            "event_type",
            "severity",
            "description",
        ]
        event.pop(random.choice(missing_field))

    # Corrupt source ip
    elif corruption == "bad_source_ip":
        event["source_ip"] = "999.999.999.999"

    # Corrupt destination ip
    elif corruption == "bad_destination_ip":
        event["destination_ip"] = "999.999.999.999"

    # Corrupt timestamp
    elif corruption == "bad_timestamp":
        event["timestamp"] = "BAD_TIMESTAMP"

    # Corrupt event type
    elif corruption == "bad_type":
        event["event_type"] = "UNKNOWN_TYPE"

    return event
