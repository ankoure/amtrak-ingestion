import boto3
import os
import logging
import sys

# Only load dotenv if not running in Lambda
if "AWS_EXECUTION_ENV" not in os.environ:
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass

AWS_PROFILE = os.environ.get("AWS_PROFILE")

# In Lambda, don't use profiles - use IAM role
if AWS_PROFILE and "AWS_EXECUTION_ENV" not in os.environ:
    session = boto3.Session(region_name="us-east-1", profile_name=AWS_PROFILE)
else:
    session = boto3.Session(region_name="us-east-1")

s3_client = session.client("s3")

AMTRAK_ENABLED = True
VIA_ENABLED = True
BRIGHTLINE_ENABLED = False
ENVIRONMENT = "PROD"


# Logging Configuration
def setup_logging():
    """
    Configure logging for the application.
    CloudWatch-friendly format that includes timestamp, level, module, and structured data.
    """
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()

    # Create root logger
    root_logger = logging.getLogger()

    # Remove existing handlers to avoid duplicates (important for Lambda)
    if root_logger.handlers:
        for handler in root_logger.handlers:
            root_logger.removeHandler(handler)

    # Set log level
    root_logger.setLevel(getattr(logging, log_level, logging.INFO))

    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, log_level, logging.INFO))

    # CloudWatch-friendly format with structured data support
    # Format: timestamp - level - logger_name - message - extra_fields
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    # Add handler to root logger
    root_logger.addHandler(handler)

    # Silence noisy AWS SDK logs
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    return root_logger


# Initialize logging
logger = setup_logging()


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for a specific module.

    Args:
        name: Usually __name__ from the calling module

    Returns:
        Logger instance configured with the application settings
    """
    return logging.getLogger(name)
