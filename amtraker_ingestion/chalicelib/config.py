"""
Configuration and Logging Module
=================================

This module provides application configuration, AWS client setup, and
logging configuration for the Amtrak Ingestion system.

Attributes
----------
s3_client : boto3.client
    Configured S3 client for AWS operations.
AMTRAK_ENABLED : bool
    Flag to enable/disable Amtrak data processing.
VIA_ENABLED : bool
    Flag to enable/disable VIA Rail data processing.
BRIGHTLINE_ENABLED : bool
    Flag to enable/disable Brightline data processing.
ENVIRONMENT : str
    Current environment (PROD or DEV).

Functions
---------
setup_logging
    Configure application logging.
get_logger
    Get a logger instance for a module.
"""

import boto3
import json
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

_FUNCTION_NAME = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "local")


class JSONFormatter(logging.Formatter):
    """JSON log formatter for structured Datadog log ingestion."""

    def format(self, record: logging.LogRecord) -> str:
        log_record: dict = {
            "timestamp": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "function_name": _FUNCTION_NAME,
            "env": ENVIRONMENT.lower(),
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)


# Logging Configuration
def setup_logging():
    """
    Configure logging for the application.
    Emits JSON-structured logs compatible with Datadog log ingestion via Forwarder.
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

    handler.setFormatter(JSONFormatter())

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


# Datadog metrics helpers
try:
    from datadog_lambda.metric import lambda_metric as _lambda_metric

    def lambda_metric(
        metric_name: str,
        value: float,
        tags: list[str] | None = None,
    ) -> None:
        _lambda_metric(metric_name, value, tags=tags or [])

except ImportError:

    def lambda_metric(  # type: ignore[misc]
        metric_name: str,
        value: float,
        tags: list[str] | None = None,
    ) -> None:
        pass


def get_dd_tags(
    provider: str | None = None,
    function_name: str | None = None,
) -> list[str]:
    """Return standard Datadog tags for a metric emission."""
    tags = [
        f"env:{ENVIRONMENT.lower()}",
        "service:amtrak-ingestion",
        f"function_name:{function_name or _FUNCTION_NAME}",
    ]
    if provider:
        tags.append(f"provider:{provider.lower()}")
    return tags
