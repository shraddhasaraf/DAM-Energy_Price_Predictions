import logging

# Configure logging
LOG_FILE = "api.log"
ERROR_LOG_FILE = "error.log"
log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
# Add a separate handler for errors and warnings
error_handler = logging.FileHandler(ERROR_LOG_FILE)
error_handler.setLevel(logging.ERROR)  # Captures both WARNING and ERROR levels
error_handler.setFormatter(logging.Formatter(log_format))

logger = logging.getLogger("API")
logger.addHandler(error_handler)