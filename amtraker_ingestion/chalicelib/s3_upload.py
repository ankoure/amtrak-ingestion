"""
S3 Upload and Download Module
==============================

This module handles all interactions with AWS S3 for data storage and
retrieval, including compression and decompression of files.

Main Functions
--------------
get_s3_json
    Download and parse a JSON file from S3
set_s3_json
    Upload a Python dict as JSON to S3
upload_todays_events_to_s3
    Upload all event files for today's service date
"""

from datetime import date, datetime, timedelta
import glob
from chalicelib.config import s3_client, get_logger
from io import BytesIO
import gzip
import os
import time
from chalicelib.constants import (
    S3_BUCKET,
    S3_DATA_TEMPLATE,
    EASTERN_TIME,
    LOCAL_DATA_TEMPLATE,
)
from chalicelib.disk import DATA_DIR
import json

logger = get_logger(__name__)


def get_s3_json(bucket_name: str, key: str) -> dict:
    """
    Download and parse a JSON file from S3.

    Parameters
    ----------
    bucket_name : str
        Name of the S3 bucket.
    key : str
        S3 object key (path to the file).

    Returns
    -------
    dict
        Parsed JSON content as a Python dictionary.

    Raises
    ------
    Exception
        If download fails or JSON parsing fails.
    """
    logger.debug(f"Downloading JSON from S3: s3://{bucket_name}/{key}")
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        content = response["Body"].read().decode("utf-8")
        data = json.loads(content)
        logger.debug(f"Successfully downloaded JSON: {len(content)} bytes")
        return data
    except Exception as e:
        logger.error(
            f"Failed to download JSON from s3://{bucket_name}/{key}: {e}",
            exc_info=True,
        )
        raise


def set_s3_json(data: dict, bucket_name: str, key: str):
    """
    Upload a Python dictionary as a JSON file to S3.

    Parameters
    ----------
    data : dict
        Python dictionary to serialize as JSON.
    bucket_name : str
        Name of the S3 bucket.
    key : str
        S3 object key (path for the file).

    Raises
    ------
    Exception
        If upload fails.
    """
    logger.debug(f"Uploading JSON to S3: s3://{bucket_name}/{key}")

    try:
        json_bytes = json.dumps(data, indent=2).encode("utf-8")

        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json_bytes,
            ContentType="application/json",
        )

        logger.debug(f"Successfully uploaded JSON: {len(json_bytes)} bytes")
    except Exception as e:
        logger.error(
            f"Failed to upload JSON to s3://{bucket_name}/{key}: {e}",
            exc_info=True,
        )
        raise


def service_date(ts: datetime) -> date:
    # In practice a None TZ is UTC, but we want to be explicit
    # In many places we have an implied eastern
    ts = ts.replace(tzinfo=EASTERN_TIME)

    if ts.hour >= 3 and ts.hour <= 23:
        return date(ts.year, ts.month, ts.day)

    prior = ts - timedelta(days=1)
    return date(prior.year, prior.month, prior.day)


def _compress_and_upload_file(fp: str):
    """Compress a file in-memory and upload to S3."""
    start_time = time.time()

    # generate output location
    # Handle both /tmp (Lambda) and data/ (local) paths
    if fp.startswith("/tmp/"):
        # In Lambda: /tmp/raw/Provider/... -> raw/Provider/...
        rp = fp.replace("/tmp/", "")
    else:
        # Local: data/raw/Provider/... -> raw/Provider/...
        rp = os.path.relpath(fp, DATA_DIR)
    s3_key = S3_DATA_TEMPLATE.format(relative_path=rp)

    logger.debug(
        f"Compressing and uploading: {fp} -> s3://{S3_BUCKET}/{s3_key}"
    )

    try:
        with open(fp, "rb") as f:
            # gzip to buffer and upload
            original_data = f.read()
            original_size = len(original_data)

            gz_bytes = gzip.compress(original_data)
            compressed_size = len(gz_bytes)
            buffer = BytesIO(gz_bytes)

            # Determine content type from file extension
            content_type = (
                "application/json" if fp.endswith(".json") else "text/csv"
            )
            s3_client.upload_fileobj(
                buffer,
                S3_BUCKET,
                Key=s3_key,
                ExtraArgs={
                    "ContentType": content_type,
                    "ContentEncoding": "gzip",
                },
            )

        compression_ratio = (
            100 * (1 - compressed_size / original_size)
            if original_size > 0
            else 0
        )
        duration = time.time() - start_time

        logger.info(
            f"Uploaded to S3: {s3_key} - "
            f"{original_size} -> {compressed_size} bytes "
            f"({compression_ratio:.1f}% compression) "
            f"in {duration:.2f}s"
        )

    except Exception as e:
        logger.error(
            f"Failed to compress and upload {fp} to s3://{S3_BUCKET}/{s3_key}: {e}",
            exc_info=True,
        )
        raise


def upload_todays_events_to_s3():
    """Upload today's events to the TM s3 bucket."""
    start_time = time.time()
    logger.info("Starting upload of today's events to S3")

    pull_date = service_date(datetime.now(EASTERN_TIME))
    logger.info(f"Service date: {pull_date}")

    # get files updated for this service date
    # TODO: only update modified files? cant imagine much of a
    # difference if we partition live data by day
    files_updated_today = glob.glob(
        LOCAL_DATA_TEMPLATE.format(
            year=pull_date.year, month=pull_date.month, day=pull_date.day
        )
    )

    file_count = len(files_updated_today)
    logger.info(f"Found {file_count} files to upload")

    # upload them to s3, gzipped
    uploaded_count = 0
    failed_count = 0

    for fp in files_updated_today:
        try:
            _compress_and_upload_file(fp)
            uploaded_count += 1
        except Exception as e:
            logger.error(f"Failed to upload file {fp}: {e}")
            failed_count += 1

    total_duration = time.time() - start_time
    logger.info(
        f"Upload completed in {total_duration:.2f}s - "
        f"Success: {uploaded_count}, Failed: {failed_count}"
    )


if __name__ == "__main__":
    upload_todays_events_to_s3()
