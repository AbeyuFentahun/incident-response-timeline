# Run this script in the terminal using: python3 -m src.transform.transform_security_events
import os
import re
import json
from datetime import datetime
from dotenv import load_dotenv
from src.utils.logger import get_logger




# Load environment variables from .env file into memory
load_dotenv("config/.env")

# Access environmental variables
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
DATA_DIR = os.getenv("DATA_DIR")

# Initialize logging and set up logging level
logger = get_logger(__name__)
logger.setLevel(LOG_LEVEL)


# Dynamically resolve file path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Directory path to retrieve raw valid data
raw_dir_path = os.path.join(BASE_DIR, DATA_DIR, "raw")
# Directory path to store transformed data
staging_dir_path = os.path.join(BASE_DIR, DATA_DIR, "staging")

# Make sure directory exists, if it doesn't create it
os.makedirs(staging_dir_path, exist_ok=True)

# Regex pattern to match timestamps in filenames (YYYYMMDD_HHMMSS)
pattern = r"\d{8}_\d{6}"

# Timestamp instance for every file
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

print(timestamp)


# Helper: returns sorted list of timestamped files (newest → oldest)
def get_timestamped_files(directory_path):
    try:
        raw_files = [file for file in os.listdir(directory_path) if re.search(pattern, file)]
    except Exception as e:
        logger.error(f"Could not list files in {directory_path}: {e}")
        return []
    
    # Makes sure the list isn't empty
    if not raw_files:
        logger.error(f"No timestamped files found in {directory_path}")
        return []

    sorted_raw_files = sorted(
        raw_files,
        # Returns the pattern match and turns it into a datetime object
        key=lambda f: datetime.strptime(
            re.search(pattern, f).group(), "%Y%m%d_%H%M%S"
        ),
        reverse=True
    )

    return sorted_raw_files

def transform_security_events():
    # Run helper function to get sorted raw files list
    raw_files = get_timestamped_files(raw_dir_path)
    
    # Safe guard against empty list
    if not raw_files:
        print("No raw files available for transformation.")
        raise ValueError("No raw files available — transform step aborted.")
    
    # Find most recent raw files
    latest_file = raw_files[0]
    # Dynamically creates raw file path
    raw_file_path = os.path.join(raw_dir_path, latest_file)
    # Stores transformed records
    transformed_records = []


    try:
        with open(raw_file_path, "r", encoding="utf-8") as f:
            # turn JSON response into Python Object
            raw_data = json.load(f)

        # Make sure resposne is a list
        if not isinstance(raw_data, list):
            print(f"Expected list of records in {raw_file_path}, but got {type(raw_data)}.")
            raise ValueError(f"Expected list of records in {raw_file_path}, but got {type(raw_data)}.")
        
        # Fail fast: Make sure list is not empty
        if len(raw_data) == 0:
            print(f"No valid records found in {raw_file_path}. Failing fast.")
            raise ValueError("Zero valid records — transform step aborted.")

        for record in raw_data:
            try:
                # TIMESTAMP NORMALIZATION
                # Extract timestamp (ISO-8601 from EXTRACT step)
                ts = record["timestamp"]

                if ts.endswith("Z"):
                    ts = ts[:-1] + "+00:00"

                # Convert normalized string → datetime object
                # This ensures Postgres always gets a valid timestamp
                event_time = datetime.fromisoformat(ts)

            except Exception as e:
                print(f"Error: {e}")



        



    except Exception as e:
        print(f"Error: {e}")
        
    








    



# Locate and load the RAW files (from local or S3)
# Load the JSON file(s) into memory
# Apply transformation rules (standardize + enrich)
# Validate the transformed records (staging schema validation)
# Upload transformed data to the STAGING layer