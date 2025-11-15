import polars as pl
from datetime import datetime, timezone
from chalicelib.disk import service_date
import json
from chalicelib.s3_upload import _compress_and_upload_file
import os
from chalicelib.constants import Provider
from chalicelib.config import get_logger
import time

logger = get_logger(__name__)


def calculate_service_date_from_datetime(dt: datetime) -> str:
    """
    Calculate service date from datetime object.

    The service_date is calculated based on the event time:
    - If hour is between 3 and 23, use the same date
    - If hour is 0-2, use the previous day

    Args:
        dt: Python datetime object (can be timezone-aware)

    Returns:
        Service date in ISO format (YYYY-MM-DD), or None if input is None
    """
    if dt is None:
        return None
    svc_date = service_date(dt)
    return svc_date.isoformat()


def add_service_dates(enriched_df: pl.DataFrame) -> pl.DataFrame:
    """
    Add service_date columns for arrival and departure events.

    Args:
        enriched_df: DataFrame with 'arr' and 'dep' datetime columns

    Returns:
        DataFrame with added 'service_date_arr' and 'service_date_dep' columns
    """
    # Add service_date columns using polars map_elements
    df_with_dates = enriched_df.with_columns(
        [
            pl.col("arr")
            .map_elements(
                calculate_service_date_from_datetime, return_dtype=pl.Utf8
            )
            .alias("service_date_arr"),
            pl.col("dep")
            .map_elements(
                calculate_service_date_from_datetime, return_dtype=pl.Utf8
            )
            .alias("service_date_dep"),
        ]
    )

    return df_with_dates


def write_amtraker_events(
    enriched_df: pl.DataFrame, mode: Provider | str = Provider.AMTRAK
):
    """
    Generate and write arrival/departure events from enriched data.

    Args:
        enriched_df: Polars DataFrame with enriched Amtraker data
                     (must include direction_id, scheduled_headway,
                     scheduled_tt, and service_date columns)
        mode: Provider name for output directory
    """
    start_time = time.time()
    input_rows = len(enriched_df)
    logger.info(f"Writing events for {mode}: {input_rows} input rows")

    # Add service_date columns first
    df_with_service_dates = add_service_dates(enriched_df)

    # Convert DataFrame to list of dictionaries for easier processing
    records = df_with_service_dates.to_dicts()

    list_of_dicts = []
    arrival_count = 0
    departure_count = 0

    for record in records:
        # Create arrival event
        arr_time = record.get("arr")
        if arr_time:
            # Convert datetime to ISO format string if needed
            if isinstance(arr_time, datetime):
                arr_time_str = arr_time.isoformat()
            else:
                arr_time_str = arr_time

            arrival_event = {
                "service_date": record.get("service_date_arr"),
                "route_id": record.get("routeName"),
                "trip_id": record.get("trainNumRaw"),
                "direction_id": record.get("direction_id"),
                "stop_id": record.get("code"),
                "stop_sequence": None,
                "vehicle_id": record.get("trainNumRaw"),
                "vehicle_label": record.get("trainNumRaw"),
                "event_type": "ARR",
                "event_time": arr_time_str,
                "scheduled_headway": record.get("scheduled_headway"),
                "scheduled_tt": record.get("scheduled_tt"),
            }
            list_of_dicts.append(arrival_event)
            arrival_count += 1

        # Create departure event
        dep_time = record.get("dep")
        if dep_time:
            # Convert datetime to ISO format string if needed
            if isinstance(dep_time, datetime):
                dep_time_str = dep_time.isoformat()
            else:
                dep_time_str = dep_time

            departure_event = {
                "service_date": record.get("service_date_dep"),
                "route_id": record.get("routeName"),
                "trip_id": record.get("trainNumRaw"),
                "direction_id": record.get("direction_id"),
                "stop_id": record.get("code"),
                "stop_sequence": None,
                "vehicle_id": record.get("trainNumRaw"),
                "vehicle_label": record.get("trainNumRaw"),
                "event_type": "DEP",
                "event_time": dep_time_str,
                "scheduled_headway": record.get("scheduled_headway"),
                "scheduled_tt": record.get("scheduled_tt"),
            }
            list_of_dicts.append(departure_event)
            departure_count += 1

    json_string = json.dumps(list_of_dicts)
    json_size = len(json_string)
    date = datetime.now(timezone.utc)

    mode_str = str(mode)
    # Use /tmp in Lambda, otherwise use data/ directory
    base_dir = "/tmp" if "AWS_EXECUTION_ENV" in os.environ else "data"
    fname = (
        f"{base_dir}/raw/{mode_str}/"
        f"{date.strftime('Year=%Y/Month=%m/Day=%d/_%H_%M')}.json"
    )
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(fname), exist_ok=True)

    logger.debug(f"Writing events to file: {fname}")

    try:
        with open(fname, "w") as file:
            file.write(json_string)

        logger.debug(f"File written successfully: {json_size} bytes")

        # Compress and upload to S3
        _compress_and_upload_file(fname)

        duration = time.time() - start_time
        logger.info(
            f"Events written and uploaded for {mode} in {duration:.2f}s - "
            f"{arrival_count} arrivals, {departure_count} departures "
            f"({len(list_of_dicts)} total events, {json_size} bytes)"
        )

    except Exception as e:
        logger.error(f"Error writing events for {mode}: {e}", exc_info=True)
        raise
