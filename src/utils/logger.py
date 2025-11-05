# Run script in terminal: python3 -m src.utils.db_connection
# python3 says Find and run the module called src.utils.db_connection from the project root.
import logging
import time
import os
import datetime
import inspect 
from dotenv import load_dotenv 
from logging.handlers import RotatingFileHandler

# Load environment variables from .env file into memory
load_dotenv("config/.env")
ENVIRONMENT = os.getenv("ENVIRONMENT", "local").lower()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Creates the logic and functionality to create logs. 
# This allows us to call the function without having to rewrite it multiple times
# Parameter can be equal to None
def get_logger(name=None):
    # ----------------------------------------
    # Detect the caller if __name__ == "__main__"
    # ----------------------------------------
    # When running a file directly, __name__ = "__main__"
    # This causes logs to be written to __main__.log instead of the correct module log.
    # The logic below dynamically detects the actual caller module or script path.
    if not name or name == "__main__":
        # returns the function at the specific call stack
        frame = inspect.stack()[1]
        # returns the module where the function ran in the call stack
        module = inspect.getmodule(frame[0])

        if module and module.__name__ != "__main__":
            name = module.__name__
        else:
            # Fallback: derive from relative file path
            name = os.path.splitext(os.path.relpath(frame.filename, start=os.getcwd()))[0]
            name = name.replace(os.sep, ".")  # Convert path -> module style

   # Derive the log path dynamically from the module name
    log_path = name.replace("src.", "").replace(".", "/")

    # Base directory (project root)
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    log_dir = os.path.join(BASE_DIR, "logs", os.path.dirname(log_path))
    os.makedirs(log_dir, exist_ok=True)

    # Use one persistent file per module (no timestamp)
    log_file = os.path.join(log_dir, f"{os.path.basename(log_path)}.log")



    # Make sure directory/file path exists before logging
    # If it doesn't exist create the directory/file path
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # It prevents duplicate logger objects by checking if it logger object exists from the memory, if it does exist, return the logger object, 
    # If it doesn't exist, create the logger object
    logger = logging.getLogger(name)
    # Filters what will be written in my log files
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    # Prevent log duplication from parent loggers
    logger.propagate = False

    # Prevent duplicate handlers (so logs don’t appear twice)
    # If the logger handler doesn't exists, create it
    # logger.handlers deals with how and where the logs are sent
    # Where could be file, console, email, API
    # How could be written, sent, streamed, rotated, or discarded
    if not logger.handlers:
        # Determines how the log will look
        formatter = logging.Formatter(
            "%(asctime)s | %(name)s | %(levelname)s | %(message)s", 
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        formatter.converter = time.gmtime  # force UTC timestamps

        # Tells where the log will be written (not connected yet so nothing will be written until logger.addHandler())
        file_handler = RotatingFileHandler(
            log_file, maxBytes=5_000_000, backupCount=5, encoding="utf-8"
        )

        # Sets up the format of the log
        file_handler.setFormatter(formatter)
        # Tells where the log will be written through the file handler variable and connect its (writes to the log to the file)
        logger.addHandler(file_handler)

        # Stream handler — prints logs to the console
        # sends the logs to the terminal/console
        stream_handler = logging.StreamHandler()
        # sets the format for the logs to the terminal/console
        stream_handler.setFormatter(formatter)
        # Tells where the log will be written through the stream handler variable and connect it (writes it the console)
        logger.addHandler(stream_handler)
    
    # return logger object so it can be used in the other modules so no set up is required
    return logger
