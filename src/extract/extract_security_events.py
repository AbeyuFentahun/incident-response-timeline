# Run this script in the terminal using: python3 -m src.extract.extract_security_events
import os
import json
import datetime
from dotenv import load_dotenv
from src.utils.logger import get_logger


# Load environment variables from .env file into memory
load_dotenv("config/.env")


# Load LOG_LEVEL and ENVIRONMENT vars from .env
# Log levels are essentially labels that indicate the severity or urgency of the various events in your application
# LOG_LEVEL allows us to dynamically control which events are logged based on severity (INFO, DEBUG, ERROR, etc.)
# ENVIRONMENT helps differentiate between environments like local, staging, or production for contextual logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
ENVIRONMENT = os.getenv("ENVIRONMENT", "local").lower()
DATA_DIR = os.getenv("DATA_DIR")



# Initialize logger and set its level based on .env
# __name__ ensures logs identify which module generated the message (e.g., src.utils.db_connection)
logger = get_logger(__name__)
logger.setLevel(LOG_LEVEL)


# Timestamp instance for every file
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

# Dynamical resolve file paths
# Get root directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# File path to extract mock data
file_path = os.path.join(BASE_DIR, DATA_DIR, "raw", "mock_security_events.json")

# File path to store valid mock data
valid_output_path = os.path.join(BASE_DIR, DATA_DIR, "raw", f"mock_security_events_{timestamp}.json")

# File path to store invalid mock data
invalid_output_path = os.path.join(BASE_DIR, DATA_DIR, "dead_letter", f"mock_security_events_{timestamp}.json")

# Creates directory if it doesn't exist; if it does, ignore
os.makedirs(os.path.dirname(valid_output_path), exist_ok=True)
os.makedirs(os.path.dirname(invalid_output_path), exist_ok=True)


# Required fields in response
required_fields = [
    "event_id", 
    "timestamp", 
    "source_ip", 
    "destination_ip", 
    "event_type", 
    "severity", 
    "description"
    ]


def extract_data(file):
    # Store valid records from response
    valid_records = []
    # Store invalid records from response
    invalid_records = []
    try:
        with open(file, "r", encoding="utf-8") as input_file:
            # Parse JSON into Python Object
            data = json.load(input_file)
            if not isinstance(data, list):
                logger.error(f"Unexpected JSON structure: expected list, got {type(data)}")
                raise ValueError("Invalid JSON structure")

            logger.info("JSON parsed into Python Object")
            # Loops through the list of objects in Python Object
            for record in data:
                # Creates a list
                # Checks if the field in required_field exists in the current record in data
                # Recreated during each iteration
                # Loops through record before executing the if statement
                missing = [field for field in required_fields if field not in record]
                # if field(s) missing, append to the invalid record list
                if missing:
                    invalid_records.append(
                        {
                            "record_id": record.get("event_id", "<no_id>"),
                            "missing_keys": missing
                        }
                    )
                else:
                    # If fields exists, append the record to valid records list
                    valid_records.append(record)


            # If valid records exist, create a new file and write the valid records to the new file   
            if valid_records:
                with open(valid_output_path, "w", encoding="utf-8") as valid_file:
                        json.dump(valid_records, valid_file, indent=2)
                        logger.info(f"{len(valid_records)} valid records written to {valid_output_path}")
            else:
                logger.error("No Valid Records found. Extraction aborted.")
                raise ValueError("No valid records found in the dataset.")


            
            # If invalid records exist, create a new file and write the invalid records to the new file   
            if invalid_records:
                with open(invalid_output_path, "w", encoding="utf-8") as invalid_file:
                    json.dump(invalid_records, invalid_file, indent=2)
                logger.info(f"{len(invalid_records)} invalid records written to {invalid_output_path}")


    # Catch errors and logs error to the log file
    except FileNotFoundError as e:
        logger.error(f"File not found: {file_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"JSON parsing failed: {e}")
        raise
    except PermissionError as e:
        logger.error(f"Permission denied reading {file_path}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

    # Logs how many records were processed
    logger.info(
        f"Extraction summary → total: {len(data)}, "
        f"valid: {len(valid_records)}, invalid: {len(invalid_records)}"
    )

    # Return paths so you can dynamically receive, reuse, and pass them between stages
    return valid_output_path, invalid_output_path



# Allows you to run smoke tests on the db connection whenever this file is executed directly
# If this file is being executed directly, run this
# If this file is being imported, don't run this
if __name__ == "__main__":
    try:
        extract_data(file_path)
        logger.info("Extraction completed successfully.")
    except Exception:
        logger.exception("Extraction failed.")
        raise

    




