import polars as pl
from datetime import datetime, timezone
from chalicelib.config import s3_client
import json
from chalicelib.constants import S3_BUCKET


def filter_events(df: pl.DataFrame, date_column: str) -> pl.DataFrame:
    """
    This function filters the dataframe based the last processed datetime.

    Args:
        df (pl.DataFrame): Input Polars Dataframe.
        date_column (str): String value representing date column name.

    Returns:
        pl.DataFrame: The filtered DataFrame or the same dataframe if the process is starting fresh.
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
    """Store the current UTC time as the last processed timestamp in S3."""
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
    """Retrieve the last processed timestamp from S3 (or None if missing)."""
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key="last_checked.json")
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
