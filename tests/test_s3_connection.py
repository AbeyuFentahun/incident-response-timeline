# Run script in terminal : python3 tests/test_s3_connection.py
import os # Lets you read env variables via os.getenv()
from dotenv import load_dotenv # Loads env vars into memory
import boto3 # Used to create, configure, and manage AWS services and resources

# Load environment variables from .env file into memory
load_dotenv("config/.env")

# Access environment variables
# AWS environment variables
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")

# Make sure env variables exists
required_vars = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "S3_BUCKET"]
for var in required_vars:
    # Checks if the value exists
    if not os.getenv(var):
        # Raise an error if it doesn't exist
        raise ValueError(f"Missing environment variable: {var}")

# Try to connect to s3 bucket API
try:
    s3 = boto3.client(
    "s3", # The type of AWS service you want to connect to
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)
    # Checks if the bucket exists and you have access via .head_bucket()
    s3.head_bucket(Bucket=S3_BUCKET) 
    print(f"✅ Connection to S3 bucket '{S3_BUCKET}' in region '{AWS_REGION}' successful.")
# Prints the error if an error occurs
except Exception as e:
    print(f"Error connecting to S3 bucket: {e}")


