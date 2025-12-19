from dotenv import load_dotenv
from src.utils.logger import get_logger

required_response_fields = [
    "events",
    "size",
    "fault_rate",
    "valid_events",
    "invalid_events"
]


logger = get_logger(__name__)



def validate_api_response(data, fault_rate):

    # Make sure response is a dictionary
    if not isinstance(data, dict):
        raise ValueError(f"Data needs to be an object. Current Data Type is: {type(data)}")
    
    # List comprehension
    # Checks if there any missing keys in response
    missing_keys = [key for key in required_response_fields if key not in data]
    
    if missing_keys:
        logger.error(
        "Missing required keys in API response",
        extra={
            "missing_keys": missing_keys,
            "received_keys": list(data.keys())
        }
            )
        raise ValueError(f"Missing keys in response: {missing_keys}")

    
    
    # Makes sure data["events"] is a list
    if not isinstance(data["events"], list):
        raise ValueError(f"events needs to be an list. Current Data Type is: {type(data['events'])}")
    
    # Make sure data["size"] is a int
    if not isinstance(data["size"], int):
        raise ValueError(f"size needs to be an int. Current Data Type is: {type(data['size'])}")
    
    # Make sure data["valid_events"] is an int
    if not isinstance(data["valid_events"], int):
        raise ValueError(f"valid_events needs to be an int. Current Data Type is: {type(data['valid_events'])}")
    
    # Make sure data["invalid_events"] is a int
    if not isinstance(data["invalid_events"], int):
        raise ValueError(f"invalid_events needs to be an int. Current Data Type is: {type(data['invalid_events'])}")
    
    # Safe float comparison with tolerance
    tolerance = 1e-6
    if not isinstance(data["fault_rate"], (int, float)):
        raise ValueError(f"fault_rate needs to be a float. Current Data Type is {type(data['fault_rate'])}")
    
    # Normalize to float
    received_rate = float(data["fault_rate"])

    if abs(received_rate - fault_rate) > tolerance:
        raise ValueError(
        f"fault_rate mismatch. Sent: {fault_rate}, Received: {received_rate}"
            )
    
    return True
