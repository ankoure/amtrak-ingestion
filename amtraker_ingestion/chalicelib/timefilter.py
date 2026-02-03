"""
Time-based Event Filtering Module
==================================

This module provides time-based filtering of events to avoid processing
duplicates. It tracks the last processed timestamp in S3 and filters
events to only include those newer than this timestamp.

Main Functions
--------------
filter_events
    Filter DataFrame to only include events after last processed time
get_last_processed
    Retrieve the last processed timestamp from S3
set_last_processed
    Store the current time as the last processed timestamp
"""

import polars as pl
from datetime import datetime, timezone
from chalicelib.config import s3_client
import json
from chalicelib.constants import S3_BUCKET


def filter_events(df: pl.DataFrame, date_column: str) -> pl.DataFrame:
    """
    Filter DataFrame to only include events after the last processed time.

    Retrieves the last processed timestamp from S3 and filters the
    DataFrame to only include rows where the specified date column
    is newer than this timestamp.

    Parameters
    ----------
    df : polars.DataFrame
        Input DataFrame with event data.
    date_column : str
        Name of the datetime column to filter on (e.g., "dep").

    Returns
    -------
    polars.DataFrame
        Filtered DataFrame with only new events, or the original
        DataFrame if no previous timestamp exists.
    """
    cutoff_date = get_last_processed()
    if cutoff_date:
        # Convert both to UTC for comparison
        filtered_range_df = df.filter(
            pl.col(date_column).dt.convert_time_zone("UTC") > cutoff_date
        )
    else:
        filtered_range_df = df

    return filtered_range_df


def set_last_processed():
    """
    Store the current UTC time as the last processed timestamp in S3.

    Saves the current timestamp to ``last_checked.json`` in the configured
    S3 bucket. This timestamp is used by :func:`filter_events` to avoid
    reprocessing events.

    Returns
    -------
    datetime
        The timestamp that was stored.
    """
    current_utc_time = datetime.now(timezone.utc)
    time_dict = {"datetime": current_utc_time.isoformat()}
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key="last_checked.json",
        Body=json.dumps(time_dict).encode("utf-8"),
        ContentType="application/json",  # Important for proper content type
    )
    return current_utc_time


def get_last_processed() -> None | datetime:
    """
    Retrieve the last processed timestamp from S3.

    Reads the ``last_checked.json`` file from S3 and returns the stored
    timestamp. Returns None if the file doesn't exist (first run).

    Returns
    -------
    datetime or None
        The last processed timestamp, or None if not found.
    """
    try:
        response = s3_client.get_object(
            Bucket=S3_BUCKET, Key="last_checked.json"
        )
        body = response["Body"].read().decode("utf-8")
        time_dict = json.loads(body)
        timestamp_str = time_dict.get("datetime")

        if timestamp_str:
            return datetime.fromisoformat(timestamp_str)
        else:
            return None

    except s3_client.exceptions.NoSuchKey:
        print("No existing last_checked.json found in S3 — starting fresh.")
        return None
    except Exception as e:
        print(f"Error reading last_checked.json: {e}")
        return None


if __name__ == "__main__":
    set_last_processed()
    print(get_last_processed())
