# Run this script in terminal: python3 -m src.extract.extract_security_events
import os
import requests
import json
from datetime import datetime
from dotenv import load_dotenv
from src.utils.logger import get_logger
from src.extract.s3_uploader import upload_to_s3
from src.validation.validate_api_response import validate_api_response


# Load environmental variables into OS memory
load_dotenv("config/.env")

# Access environmental variables
ENVIRONMENT = os.getenv("ENVIRONMENT")
DATA_DIR = os.getenv("DATA_DIR")
API_BASE_URL = os.getenv("API_BASE_URL")
API_KEY = os.getenv("API_KEY")


# Initialize get_logger()
# Set logging level
logger = get_logger(__name__)

# Dynamical resolve file paths
# Get root directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ENDPOINT FOR API that request will be made to
url = f"{API_BASE_URL}/events/batch"
# HEADER FOR AUTHORIZED USER / REQUESTS
headers = {"x-api-key": API_KEY}


# Creates directory if it doesn't exist; if it does, ignore
os.makedirs(os.path.join(BASE_DIR, "data", "raw"), exist_ok=True)


def extract_data(size, fault_rate):

    # Timestamp instance for files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Dynamically resolves file path to data/raw
    raw_output_path = os.path.join(BASE_DIR, DATA_DIR, "raw", f"raw_events_{timestamp}.json")

    # Fast fail
    # Check if query paramters are valid
    if size < 1:
        raise ValueError("Size must be >= 1")
    if not (0.0 <= fault_rate <= 1.0):
        raise ValueError("fault_rate must be between 0.0 and 1.0")
    
    # Query parameters
    params = {"size" : size, "fault_rate" : fault_rate}

    try:
    # GET request to API endpoints
        response = requests.get(url,
                                headers=headers, 
                                params=params,
                                # 5 secs to connect API, 15 secs to get response
                                timeout=(5, 15)
                                )
        
        # Raises HTTP response
        response.raise_for_status()
        # Convert API response (JSON TEXT) into Python Object
        data = response.json()

    except requests.exceptions.RequestException as e:
        logger.error(
            "API request failed",
            extra={
                "url": url,
                "params": {"size": size, "fault_rate": fault_rate}
            }
        )
        raise

    except ValueError:  
        # JSON decode error
        logger.error("Failed to parse JSON from API response")
        raise

    # Validate API response
    validate_api_response(data, fault_rate)

    try:
    # Write data to data/raw/f"raw_events_{timestamp}.json"
        with open(raw_output_path, "w", encoding="utf-8") as f:
        # write json data to file
            json.dump(data, f, indent=4)


    except OSError as e:
        logger.error(
            "Failed to write raw file",
            extra={"path": raw_output_path}
        )
        raise


    try:
        s3_key = f"raw/{os.path.basename(raw_output_path)}"
        # upload data to s3 bucket
        upload_to_s3(raw_output_path, s3_key)

    except Exception:
        logger.error(
            "Failed to upload raw file to S3",
            extra={"path": raw_output_path, "s3_key": s3_key},
        )
        raise


    logger.info(
        "Successfully extracted raw events",
        extra={
            "path": raw_output_path,
            "s3_key": s3_key,
            "size": data["size"],
            "valid_events": data["valid_events"],
            "invalid_events": data["invalid_events"],
        },
    )


    # Return data
    return data
    
    
# Smoke test
if __name__ == "__main__":
    print("Extracting data from API")
    data = extract_data(20, .25)
    print("Response succesful and Data returned")
    print(data)



