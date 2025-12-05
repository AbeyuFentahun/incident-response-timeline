# Run this script in the terminal using: python3 -m src.utils.db_connection
# The -m flag runs the file as a module (required because it imports other modules from the same package)
import os
from dotenv import load_dotenv
import psycopg2
from src.utils.logger import get_logger # imports get_logger functionality for modulairty


# Load environment variables from .env file into memory
load_dotenv("config/.env")



# ENVIRONMENT helps differentiate between environments like local, staging, or production for contextual logging
ENVIRONMENT = os.getenv("ENVIRONMENT", "local").lower()


# Initialize logger
# __name__ ensures logs identify which module generated the message (e.g., src.utils.db_connection)
logger = get_logger(__name__)



"""
Establishes a secure connection to the AWS RDS PostgreSQL database.
Loads credentials from the .env file, validates all required environment variables,
and returns a reusable psycopg2 connection object with SSL encryption enabled.
The caller is responsible for closing the connection after use.
"""
def get_connection():

    # Access environment variables
    # POSTGRES environment variables
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    
    
    # If USE_SSL var doesn’t exist, use the default value "true" instead. Lowercase what is returned and make sure it is equal to "true"
    USE_SSL = os.getenv("USE_SSL", "true").lower() == "true"


    # Raises an error if any required environment variable is missing
    # List of required vars from .env
    required_vars = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]
    
    # Loop through required_vars
    for var in required_vars:
        # Checks if var exists in memory
        if not os.getenv(var):
            # Raise an error if it doesn't exist with the var that is missing
            raise ValueError(f"Missing environment variable: {var}")

    # If USE_SSL is True, set sslmode to "require"; otherwise, set it to "disable"
    # This allows SSL encryption to be toggled on or off via the .env configuration
    ssl_mode = "require" if USE_SSL else "disable"

    # Try to connect to AWS RDS PostgreSQL DB
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            sslmode=ssl_mode,  # Always encrypt traffic between my Python script and the database — no exceptions
            connect_timeout=10,  # Wait 10 seconds to return a connection before running the except block
        )
        # Logs successful connection to the log file
        logger.info(
            f"Connection to database '{DB_NAME}' on host '{DB_HOST}' successful."
            f"(ENV={ENVIRONMENT}, SSL={'ON' if USE_SSL else 'OFF'})."
        )
        # Returns a reusable psycopg2 connection object with SSL encryption enabled
        return conn
    
    # Catch error and logs error to the log file
    except psycopg2.Error as e:
        logger.error(f"Connection failed: {e}")
        # Check if conn exists in the local memory and if it contains a truthy value
        if "conn" in locals() and conn:
            # Closes the connection to stop leaks
            conn.close()
            # Logs warning to the log file
            logger.warning("Connection closed after failure.")
        # Stop execution immediately and bubble up the error and Airflow will decide what to do
        raise


"""

# This would be used in production to ensure connections are always closed,
# Even if an exception occurs, by placing it in a finally block.
Always run this in the end
finally:
If the conn var exists in the local memory and isn't empty close the connection
We close the connection because AWS has a certain number of concurrent connections available
if 'conn' in locals() and conn:
    conn.close()
"""


# Allows you to run smoke tests on the db connection whenever this file is executed directly
# If this file is being executed directly, run this
# If this file is being imported, don't run this
if __name__ == "__main__":
    try:
        logger.info("Starting DB setup and connectivity check...")
        conn = get_connection()
        logger.info("Connection successful.")
        conn.close()
        logger.info("Connection closed.")
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
