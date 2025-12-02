# 1. Identify the source the requests will be made to
# 2. Test the connection
# 3. Extract data
# 4. Validate data
# 5. Save data to the preferred destination


# Run this script in terminal: python3 -m src.extract.extract_security_events1
import os
import requests
import json
from datetime import datetime
from dotenv import load_dotenv
from src.utils.logger import get_logger


# Load environmental variables into OS memory
load_dotenv("config/.env")

# Access environmental variables
LOG_LEVEL = os.getenv("LOG_LEVEL")
ENVIRONMENT = os.getenv("ENVIRONMENT")
DATA_DIR = os.getenv("DATA_DIR")
API_BASE_URL = os.getenv("API_BASE_URL")
API_KEY = os.getenv("API_KEY")


# Initialize get_logger()
# Set logging level
logger = get_logger(__name__)
logger.setLevel(LOG_LEVEL)

# Dynamical resolve file paths
# Get root directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ENDPOINT FOR API that request will be made to
url = f"{API_BASE_URL}/events/batch"
# HEADER FOR AUTHORIZED USER / REQUESTS
headers = {"x-api-key": API_KEY}


# Creates directory if it doesn't exist; if it does, ignore
os.makedirs(os.path.join(BASE_DIR, "data", "raw"), exist_ok=True)


# Required keys in response
required_response_fields = [
    "events",
    "size",
    "fault_rate",
    "valid_events",
    "invalid_events"
]


def extract_data(size, fault_rate):

    # Timestamp instance for files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Dynamically resolves file path to data/raw
    raw_output_path = os.path.join(BASE_DIR, DATA_DIR, "raw", f"raw_events_{timestamp}.json")


    # Check if query paramters are valid
    if size < 1:
        raise ValueError("Size must be >= 1")
    if not (0.0 <= fault_rate <= 1.0):
        raise ValueError("fault_rate must be between 0.0 and 1.0")
    
    # paramters
    params = {"size" : size, "fault_rate" : fault_rate}

    # GET request to API endpoints
    response = requests.get(url, headers=headers, params=params)

    # Checks GET request status code to see if it was successful or unsuccessful
    if response.status_code != 200:
        print(f"API request UNSUCCESFUL")
        raise ValueError(f"API request UNSUCCESFUL {response.status_code}")
    
    # Convert API response (JSON TEXT) into Python Object
    data = response.json()


    # Make sure response is a dictionary
    if not isinstance(data, dict):
        raise ValueError(f"Data needs to be an object. Current Data Type is: {type(data)}")
    
    # List comprehension
    # Checks if there any missing keys in response
    missing_keys = [key for key in required_response_fields if key not in data]
    
    if missing_keys:
        print(f"Missing keys in response: {missing_keys}")
        raise ValueError("Missing keys in response")
    
    
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

    if abs(data["fault_rate"] - fault_rate) > tolerance:
        raise ValueError(
        f"fault_rate mismatch. Sent: {fault_rate}, Received: {data['fault_rate']}"
            )

    
    # write data to data/raw/f"raw_events_{timestamp}.json"
    with open(raw_output_path, "w", encoding="utf-8") as f:
        # write json data to file
        json.dump(data, f, indent=4)


    # Return data
    return data
    
    
# Smoke test
if __name__ == "__main__":
    print("Extracting data from API")
    data = extract_data(5, .20)
    print("Response succesful and Data returned")
    print(data)



    

    





