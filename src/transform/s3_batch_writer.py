# Run this script in terminal: python3 -m src.transform.s3_batch_writer
import os
import json
import tempfile
from datetime import datetime, timezone
from src.utils.aws_client import get_s3_client, test_s3_connection
from src.utils.logger import get_logger


# Initialize logger
logger = get_logger(__name__)

def transformed_batch_to_s3(data, s3_bucket, s3_key):
    file_path = None
    if not data:
        logger.info(
            "Skipping S3 upload — empty batch",
            extra={
                "s3_key": s3_key,
                "data_type": type(data).__name__,
            },
        )
        return  # Deliberate no-op

    try:
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp:
            json.dump(data, tmp, indent=4, default=str)
            # Make sure that the data is actually written
            tmp.flush()
            # Get file path to access it
            file_path = tmp.name
        s3 = get_s3_client()
        logger.info("S3 Client Initialized")
        # Test s3 conneciton
        test_s3_connection()
        logger.info("Connection to s3 successful!")

        s3.upload_file(file_path, s3_bucket, s3_key)
        logger.info("Temp file data uploaded successfully!")

    except Exception as e:
        logger.error("Unexpected error occurred")
        raise

    finally:
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception as e:
                logger.warning(
                    "Failed to clean up temp file after S3 upload",
                    extra={
                        "file_path": file_path,
                        "error": str(e),
                    },
                )
