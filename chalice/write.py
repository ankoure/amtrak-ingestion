import polars as pl
from datetime import datetime
from disk import write_event, service_date


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
            .map_elements(calculate_service_date_from_datetime, return_dtype=pl.Utf8)
            .alias("service_date_arr"),
            pl.col("dep")
            .map_elements(calculate_service_date_from_datetime, return_dtype=pl.Utf8)
            .alias("service_date_dep"),
        ]
    )

    return df_with_dates


def write_amtraker_events(enriched_df: pl.DataFrame) -> None:
    """
    Write enriched Amtraker data to CSV files using disk.py utilities.

    Each DataFrame row represents a train at a station with both arrival and departure times.
    This function splits each row into two events: one for arrival and one for departure.

    Args:
        enriched_df: Polars DataFrame with enriched Amtraker data (must include
                     direction_id, scheduled_headway, scheduled_tt, and service_date columns)

    Expected CSV fields (from disk.py):
        - service_date: From service_date_arr or service_date_dep
        - route_id: From routeName
        - trip_id: From trainNumRaw
        - direction_id: From direction_id
        - stop_id: From code
        - stop_sequence: Not available in Amtraker data, set to None
        - vehicle_id: From trainNumRaw
        - vehicle_label: From trainNumRaw
        - event_type: "ARR" or "DEP"
        - event_time: From arr or dep
        - scheduled_headway: From scheduled_headway
        - scheduled_tt: From scheduled_tt
        - vehicle_consist: Not available, set to None
        - occupancy_status: Not available, set to None
        - occupancy_percentage: Not available, set to None
    """
    # Add service_date columns first
    df_with_service_dates = add_service_dates(enriched_df)

    # Convert DataFrame to list of dictionaries for easier processing
    records = df_with_service_dates.to_dicts()

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
            write_event(arrival_event)

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
            write_event(departure_event)


if __name__ == "__main__":
    from read import read_amtraker_data
    from transform import add_direction_id, add_scheduled_metrics
    from utils import get_latest_gtfs_archive
    from constants import AMTRAK_STATIC_GTFS

    # Configure Polars to show all columns
    pl.Config.set_tbl_cols(-1)
    pl.Config.set_tbl_width_chars(1000)

    # Get GTFS data
    gtfs_dir = get_latest_gtfs_archive(AMTRAK_STATIC_GTFS)

    # Read and enrich Amtraker data
    amtrak_df, via_df, brightline_df = read_amtraker_data()

    # Enrich with direction_id and scheduled metrics
    enriched_amtrak = add_direction_id(amtrak_df, gtfs_dir)
    enriched_amtrak = add_scheduled_metrics(enriched_amtrak, gtfs_dir)

    # Add service dates
    df_with_service_dates = add_service_dates(enriched_amtrak)

    # Show sample with service dates
    print("\n=== Sample with Service Dates ===")
    print(
        df_with_service_dates.select(
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

    # Write events to CSV
    print(f"\nWriting {len(enriched_amtrak)} records to CSV...")
    print(f"This will create {len(enriched_amtrak) * 2} events (arrival + departure)")

    write_amtraker_events(enriched_amtrak)

    print("Done! Events written to data/ directory")
