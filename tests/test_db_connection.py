# Run script in terminal : python3 tests/test_db_connection.py
import os
from dotenv import load_dotenv
import psycopg2


# Load environment variables from .env file into memory
load_dotenv("config/.env")


# Access environment variables
# POSTGRES environment variables
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Make sure env variables exists
required_vars = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]
for var in required_vars:
    # Checks if the value exists
    if not os.getenv(var):
        # Raise an error if it doesn't exist
        raise ValueError(f"Missing environment variable: {var}")
    
    
# Try to connect to AWS RDS PostgreSQL DB
try:
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        sslmode="require" # Always encrypt traffic between my Python script and the database — no exceptions.
    )
    print(f"✅ Connection to database '{DB_NAME}' on host '{DB_HOST}' successful.")
# Catch error and print it
except psycopg2.Error as e:
    print(f"❌ Connection failed: {e}")
# Always run this in the end
finally:
    # If the conn var exists in the local memory and isn't empty close the connection
    # We close the connection because AWS has a certain number of concurrent connections available
    if 'conn' in locals() and conn:
        conn.close()

