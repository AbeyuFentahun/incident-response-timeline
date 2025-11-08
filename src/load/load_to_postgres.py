# Run this script in the terminal using: python3 -m src.load.load_to_postgres 
import os
import re
import json
from dotenv import load_dotenv
from datetime import datetime
from src.utils.db_connection import get_connection
from src.utils.logger import get_logger



# Load environmental variables in OS memory
load_dotenv("config/.env")

# Access environmental variables
DATA_DIR = os.getenv("DATA_DIR")
LOG_LEVEL = os.getenv("LOG_LEVEL")

# Initialize logger and set its level based on .env
# __name__ ensures logs identify which module generated the message (e.g., src.utils.db_connection)
logger = get_logger(__name__)
logger.setLevel(LOG_LEVEL)

# Dynamical resolve file path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Create path to directory
valid_directory_path = os.path.join(BASE_DIR, DATA_DIR, "raw")
invalid_directory_path = os.path.join(BASE_DIR, DATA_DIR, "dead_letter")



# Allows us to check if the file contains a timestamp
# Used re module because "in" compares exact text, not pattern
pattern = r"\d{8}_\d{6}"

# Loops through directory to see if the file contains a timestamp
valid_file_list = [file for file in os.listdir(valid_directory_path) if re.search(pattern, file)]
invalid_file_list = [file for file in os.listdir(invalid_directory_path) if re.search(pattern, file)]

# Sorts valid files by most recent timestamp
valid_files = sorted(
    valid_file_list,
    key=lambda f: datetime.strptime(re.search(pattern, f).group(), "%Y%m%d_%H%M%S"),
    reverse=True
)

# Sort invalid files by most recent timestamp
invalid_files = sorted(
    invalid_file_list,
    # Check if file contains timestamp
    # Extracts timestamp found in re.search
    # Turns the string into a actual datetime object
    key=lambda f: datetime.strptime(re.search(pattern, f).group(), "%Y%m%d_%H%M%S"),
    reverse=True
)

# Query to insert data into Postgre
INSERT_QUERY = '''
INSERT INTO raw.security_logs (
    event_id,
    event_time,
    source,
    severity,
    message,
    raw_payload,
    ingested_at
)
VALUES (
    %(event_id)s,
    %(event_time)s,
    %(source)s,
    %(severity)s,
    %(message)s,
    %(raw_payload)s,
    CURRENT_TIMESTAMP
)
ON CONFLICT (event_id) DO NOTHING;
'''

def load_json_to_postgres():
    conn = get_connection()
    cursor = conn.cursor()

    try:
        for file in valid_files:
            with open(os.path.join(valid_directory_path, file), "r", encoding="utf") as f:
                valid_data = json.load(f)
            print("Valid Data being read!")
            print(valid_data)
            print("Valid Data read successfully!")
            
            cursor.execute(INSERT_QUERY,
                   
                   {
                       "event_id": valid_data["event_id"],
                       "event_time": valid_data["timestamp"],
                       "source": valid_data["source_ip"],
                       "severity": valid_data["severity"],
                       "message": valid_data["description"],
                       "raw_payload": valid_data  
                    }
            )

        for file in invalid_files:
            with open(os.path.join(invalid_directory_path, file), "r", encoding="utf") as f:
                invalid_data = json.load(f)
            print("Invalid Data being read!")
            print(invalid_data)
            print("Invalid Data read successfully!")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        conn.close()




load_json_to_postgres()

