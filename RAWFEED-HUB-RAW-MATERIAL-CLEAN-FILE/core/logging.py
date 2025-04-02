## core/logging.py

import logging
from logging.handlers import RotatingFileHandler
import os
import sys
import codecs

# Create logs directory if it doesn't exist
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Log file path
LOG_FILE = os.path.join(LOG_DIR, "app.log")

# Custom stream handler for UTF-8 console output
class UTF8StreamHandler(logging.StreamHandler):
    def __init__(self, stream=None):
        if stream is None:
            # Ensure compatibility with IPython/Jupyter notebooks
            stream = sys.stdout
            if not hasattr(stream, "write") or isinstance(stream, codecs.StreamWriter):
                stream = codecs.getwriter("utf-8")(stream.buffer)
        super().__init__(stream)

# Logging configuration
def configure_logging():
    """
    Configure the logging system.
    """
    handlers = [
        UTF8StreamHandler(),  # Console handler with UTF-8 encoding
        RotatingFileHandler(LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"),  # File handler
    ]
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=handlers,
    )

# Initialize the logging system
configure_logging()

# Function to get a logger
def get_logger(name: str):
    """
    Returns a logger for the specified module name.
    """
    return logging.getLogger(name)

