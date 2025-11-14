from urllib.request import urlretrieve
from zipfile import ZipFile
from chalicelib.models.amtraker import TrainResponse
from tempfile import mkdtemp
import os
import shutil
import polars as pl
from chalicelib.config import s3_client
from chalicelib.constants import S3_BUCKET
from botocore.exceptions import ClientError
from contextlib import contextmanager


@contextmanager
def temp_gtfs_directory():
    """Context manager for creating and cleaning up temporary GTFS directories."""
    temp_dir = mkdtemp(prefix="gtfs_")
    try:
        yield temp_dir
    finally:
        # Clean up the temporary directory
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


def cleanup_old_gtfs_temp_dirs():
    """Clean up old GTFS temporary directories from /tmp to prevent disk space issues."""
    import glob

    # Find all gtfs_* directories in /tmp
    temp_dirs = glob.glob("/tmp/gtfs_*")

    for temp_dir in temp_dirs:
        try:
            if os.path.isdir(temp_dir):
                shutil.rmtree(temp_dir)
        except Exception as e:
            # Log but don't fail if cleanup fails
            print(f"Warning: Could not remove {temp_dir}: {e}")


def get_latest_gtfs_archive_from_cache(agency: str) -> str | None:
    """
    Downloads a GTFS bundle from S3 cache and extracts it.

    Args:
        agency: Agency name (e.g., "Amtrak", "Via", "Brightline")

    Returns:
        Path to extracted GTFS directory, or None if file doesn't exist in S3
    """
    s3_key = f"GTFS/{agency}.zip"

    # Create a named temporary directory
    temp_dir = mkdtemp(prefix="gtfs_")
    gtfs_zip = os.path.join(temp_dir, "gtfs.zip")

    try:
        # Download the GTFS zip file from S3
        s3_client.download_file(S3_BUCKET, s3_key, gtfs_zip)
    except ClientError as e:
        # Check if the error is because the file doesn't exist
        if (
            e.response["Error"]["Code"] == "404"
            or e.response["Error"]["Code"] == "NoSuchKey"
        ):
            return None
        # Re-raise other errors
        raise

    # Extract the GTFS archive
    gtfs_extract_path = os.path.join(temp_dir, "gtfs")
    with ZipFile(gtfs_zip, "r") as zObject:
        zObject.extractall(path=gtfs_extract_path)

    return gtfs_extract_path


def get_latest_gtfs_archive(GTFS_URL: str) -> str:
    # Create a named temporary directory
    temp_dir = mkdtemp(prefix="gtfs_")

    # Download the GTFS zip file to a temporary location
    gtfs_zip, _ = urlretrieve(GTFS_URL, os.path.join(temp_dir, "gtfs.zip"))

    # Extract the GTFS archive
    gtfs_extract_path = os.path.join(temp_dir, "gtfs")
    with ZipFile(gtfs_zip, "r") as zObject:
        zObject.extractall(path=gtfs_extract_path)

    return gtfs_extract_path


def trains_to_list(train_response: TrainResponse) -> list[dict]:
    """Convert TrainResponse to a list of Train dictionaries."""
    train_list: list[dict] = []
    for trains in train_response.root.values():
        for train in trains:
            train_list.append(train.model_dump())
    return train_list


def display_results_in_console(df: pl.DataFrame, agency_name: str):
    with pl.Config(tbl_cols=-1, tbl_width_chars=1000):
        print(f"\nWriting {len(df)} records to CSV...")
        print(f"This will create {len(df) * 2} events (arrival + departure)")

        # Show sample agency_name with service dates
        print(f"\n=== Sample {agency_name} with Service Dates ===")
        print(
            df.select(
                [
                    "trainNumRaw",
                    "code",
                    "arr",
                    "dep",
                    "service_date_arr",
                    "service_date_dep",
                ]
            ).head(10)
        )
