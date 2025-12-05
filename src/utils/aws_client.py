# Run this script in the terminal using: python3 -m src.utils.aws_client
# The -m flag runs the file as a module (required because it imports other modules from the same package)
import os
from dotenv import load_dotenv
import boto3  # Used to create, configure, and manage AWS services and resources
from src.utils.logger import get_logger # imports get_logger functionality for modulairty


# Intialize logger functionality
logger = get_logger(__name__)



# =============================== SETS UP/INITIALIZES CONNECTION  ==================================== #
# DOESN'T TEST THE CONNECTION
# Initializes and returns a boto3 S3 client object using credentials from .env.
def get_s3_client():
    # Access environment variables
    # AWS environment variables
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_REGION = os.getenv("AWS_REGION")
    S3_BUCKET = os.getenv("S3_BUCKET")

    # Raises an error if any required environment variable is missing
    # List of required vars from .env
    required_vars = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_REGION",
        "S3_BUCKET",
    ]
    # Loop through required_vars
    for var in required_vars:
        # Checks if var exists in memory
        if not os.getenv(var):
            # Stop execution immediately and bubble up the error and Airflow will decide what to do
            raise ValueError(f"Missing environment variable: {var}")

    # Returns a boto3 S3 client object after all checks
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )


# ===================================== TESTS S3 BUCKET CONNECTION =============================== #
def test_s3_connection():
    # Access environment variables
    S3_BUCKET = os.getenv("S3_BUCKET")
    AWS_REGION = os.getenv("AWS_REGION")
    # Initializes the boto3 S3 client object from get_s3_client()
    s3 = get_s3_client()

    # Try to connect to s3 bucket API
    try:
        # Checks if the bucket exists and you have access via .head_bucket()
        # .head_bucket() tests the connection
        s3.head_bucket(Bucket=S3_BUCKET)
        # Logs successful connection to the log file
        logger.info(
            f"Connection to S3 bucket '{S3_BUCKET}' in region '{AWS_REGION}' successful. "
            f"(ENV={os.getenv('ENVIRONMENT', 'local')}, REGION={AWS_REGION})."
        )
    # Catches error and logs error to the log file
    except Exception as e:
        logger.error(f"Error connecting to S3 bucket: {e}")
        # Stop execution immediately and bubble up the error and Airflow will decide what to do
        raise


# ===================================== CREATES S3 BUCKET FOLDERS =============================== #
# Creates the folder structure (prefixes) in the configured S3 bucket
# Mirrors the local data/ directory layout
def create_s3_structure():
    # Initializes the boto3 S3 client object from get_s3_client()
    s3 = get_s3_client()
    # Access environment variables
    S3_BUCKET = os.getenv("S3_BUCKET")

    # List of folders that need to be created in s3 bucket
    folders = ["raw/", "staging/", "archive/", "dead_letter/"]

    try:
        # Loops through folders list
        for folder in folders:
            # Check if folder/key already exists by listing up to 1 key with that prefix
            response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=folder, MaxKeys=1)
            # "Contents" is key inside of the response object whose value is a list of objects
            # Check if "Contents" key exists in response
            # .get() returns None instead of KeyError
            if response.get("Contents"):
                # Logs the object exists already to the log file
                logger.info(
                    f"Folder already exists: s3://{S3_BUCKET}/{folder} "
                    f"(ENV={os.getenv('ENVIRONMENT', 'local')}, REGION={os.getenv('AWS_REGION')})."
                )
            else:
                # Create folder since it doesn't exist yet
                s3.put_object(Bucket=S3_BUCKET, Key=folder)
                # Logs successful creation of the object to the log file
                logger.info(
                    f"Created folder: s3://{S3_BUCKET}/{folder} "
                    f"(ENV={os.getenv('ENVIRONMENT', 'local')}, REGION={os.getenv('AWS_REGION')})."
                )
        # Logs succesful verificaiton or creation to the log file
        logger.info(
            f"S3 folder structure verified/created in bucket '{S3_BUCKET}'. "
            f"(ENV={os.getenv('ENVIRONMENT', 'local')}, REGION={os.getenv('AWS_REGION')})."
        )
    # Catch error and logs error to the log file
    except Exception as e:
        logger.error(f"Failed to create or verify S3 folder structure: {e}")
        # Stop execution immediately and bubble up the error and Airflow will decide what to do
        raise


# Allows you to run smoke tests on the db connection whenever this file is executed directly
# If this file is being executed directly run this
# If this file is being imported don't run this
if __name__ == "__main__":
    try:
        logger.info("Starting S3 connectivity and setup check...")
        # Initialize the S3 client (verifies credentials are valid)
        get_s3_client()
        logger.info("S3 client initialized successfully.")
        # Test the actual S3 bucket connection and access permissions
        test_s3_connection()
        # Optional: create folder structure if connection succeeds
        create_s3_structure()
        logger.info("All S3 checks and setup completed successfully.")
    # Catches error and logs error to the log file
    except Exception as e:
        logger.error(f"S3 setup failed: {e}")
        raise
