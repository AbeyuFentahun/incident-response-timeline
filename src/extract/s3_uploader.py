# Run this script in the terminal using: python3 -m src.extract.s3_uploader
import os
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from src.utils.logger import get_logger
from src.utils.aws_client import get_s3_client
from src.utils.aws_client import test_s3_connection
from src.utils.aws_client import create_s3_structure


# Load environment variables from .env file into memory
load_dotenv("config/.env")


# Load environment variables from .env file into memory
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
ENVIRONMENT = os.getenv("ENVIRONMENT", "local").lower()
S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION")



# Load LOG_LEVEL and ENVIRONMENT vars from .env
# Log levels are essentially labels that indicate the severity or urgency of the various events in your application.
# LOG_LEVEL allows us to dynamically control which events are logged based on severity (INFO, DEBUG, ERROR, etc.)
# ENVIRONMENT helps differentiate between environments like local, staging, or production for contextual logging.
logger = get_logger(__name__)
logger.setLevel(LOG_LEVEL)




def upload_to_s3(local_path, s3_key):
    try:
        # Validate local file existence before doing any AWS work
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local file not found: {local_path}")
        
        
        # Sets up conifgurations for s3 bucket
        logger.info("Initializing S3 client...")
        # Initializes a boto3 S3 client instance to access S3 methods
        s3 = get_s3_client()
        logger.info("S3 client initialized successfully.")


        logger.info("Testing S3 bucket connection...")
        # Tests s3 connection
        test_s3_connection()
        logger.info("Connection to S3 bucket successful.")


        logger.info("Verifying or creating S3 folder structure...")
        # Creates or verifies s3 folder structure
        create_s3_structure()
        logger.info("S3 folder structure verified or created successfully.")

        logger.info(f"Uploading file → {local_path} → s3://{S3_BUCKET}/{s3_key}")
        # Uploads data into the specified S3 key within the bucket
        # local_path tells which file to upload
        # s3_bucket tells which s3 bucket to upload it to
        # s3_key tells which key to upload the data as a value 
        s3.upload_file(local_path, S3_BUCKET, s3_key)
        logger.info(f"File uploaded successfully: s3://{S3_BUCKET}/{s3_key}")

    # Catch error and logs error to the log file
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        raise
    except ClientError as e:
        logger.error(f"AWS ClientError while uploading to S3: {e}")
        raise
    except ValueError as e:
        logger.error(f"ValueError during upload: {e}")
        raise
    except TypeError as e:
        logger.error(f"TypeError during upload: {e}")
        raise
    except Exception as e:
        logger.exception(f"Unexpected error during S3 upload: {e}")
        raise


# Allows you to run a standalone S3 upload smoke test
# If this file is being executed directly run this
# If this file is being imported don't run this
if __name__ == "__main__":
    try:
        logger.info("Starting standalone S3 upload test...")
        # Example test file
        test_local_path = "data/raw/mock_security_events_20251105_1200.json"
        test_s3_key = f"raw/{os.path.basename(test_local_path)}"

        upload_to_s3(test_local_path, test_s3_key)

        logger.info("Standalone S3 upload test completed successfully.")
    except Exception as e:
        logger.exception(f"Standalone S3 upload test failed: {e}")
        raise

