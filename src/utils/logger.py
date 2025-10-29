# Run script in terminal: python3 -m src.utils.db_connection
# python3 says Find and run the module called src.utils.db_connection from the project root.
import logging
import os

# Creates the logic and functionality to create logs. 
# This allows us to call the function without having to rewrite it multiple times
def get_logger(name):
    # Derive path dynamically from the parameter
    log_path = name.replace("src.", "").replace(".","/")
    # Create the actual file path
    log_file = f"logs/{log_path}.log"

    # Make sure directory/file path exists before logging
    # If it doesn't exist create  the directory/file path
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # It prevents duplicate logger objects by checking if it logger object exists from the memory, if it does exist, return the logger object, 
    # If it doesn't exist, create the logger object
    logger = logging.getLogger(name)
    # Filters what will be written in my log files
    logger.setLevel(logging.INFO)
    # Prevent log duplication from parent loggers
    logger.propagate = False

    # Prevent duplicate handlers (so logs don’t appear twice)
    # If the logger handler doesn't exists, create it
    # logger.handlers deals with how and where the logs are sent
    # Where could be file, console, email, API
    # How could be written, sent, streamed, rotated, or discarded
    if not logger.handlers:
        # Determines how the log will look
        formatter = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s")
        # Tells where the log will be written (not connected yet so nothing will be written until logger.addHandler())
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        # Sets up the format of the log
        file_handler.setFormatter(formatter)
        # Tells where the log will be written through the file handler variable and connect its (writes to the log to the file)
        logger.addHandler(file_handler)

        # Stream handler — prints logs to the console
        # sends the logs to the terminal/console
        stream_handler = logging.StreamHandler()
        # sets the format for the logs to the terminal/console
        stream_handler.setFormatter(formatter)
        # Tells where the log will be written through the stream handler varaible and connect it (writes it the console)
        logger.addHandler(stream_handler)
    
    # return logger object so it can be used in the other modules so no set up is required
    return logger

