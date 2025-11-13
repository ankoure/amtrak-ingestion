"""
GTFS Metrics Calculation Module

Calculates scheduled headways and scheduled travel times from GTFS data,
following the methodology from TransitMatters/gobble.
"""

import polars as pl
from pathlib import Path


def load_gtfs_stop_times(gtfs_dir: str) -> pl.DataFrame:
    """
    Load GTFS stop_times.txt and trips.txt, joining them together.

    Args:
        gtfs_dir: Path to directory containing GTFS files

    Returns:
        DataFrame with stop_times joined with trip information
        (route_id, direction_id)
    """
    gtfs_path = Path(gtfs_dir)

    # Load stop_times.txt
    stop_times = pl.read_csv(
        gtfs_path / "stop_times.txt",
        schema_overrides={
            "arrival_time": pl.Utf8,
            "departure_time": pl.Utf8,
        },
    )

    # Load trips.txt to get route_id and direction_id
    trips = pl.read_csv(
        gtfs_path / "trips.txt", columns=["trip_id", "route_id", "direction_id"]
    )

    # Join stop_times with trips to get route and direction info
    gtfs_stops = stop_times.join(trips, on="trip_id", how="left")

    # Convert time strings to proper time format
    # Handle times >= 24:00:00 (next day times in GTFS)
    gtfs_stops = gtfs_stops.with_columns(
        [
            pl.col("arrival_time")
            .str.strptime(pl.Time, "%H:%M:%S", strict=False)
            .alias("arrival_time"),
            pl.col("departure_time")
            .str.strptime(pl.Time, "%H:%M:%S", strict=False)
            .alias("departure_time"),
        ]
    )

    return gtfs_stops


def calculate_scheduled_headway(gtfs_stops: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate scheduled headway for each stop.

    Scheduled headway is the time gap between consecutive scheduled vehicles
    at the same stop, for the same route and direction.

    Methodology from gobble/src/gtfs.py:364-369:
    - Sort by arrival_time
    - Group by (route_id, direction_id, stop_id)
    - Calculate difference between consecutive arrival times
    - First stop of a trip has no previous vehicle, so it's null

    Args:
        gtfs_stops: DataFrame with GTFS stop times and trip information

    Returns:
        DataFrame with additional 'scheduled_headway' column (in seconds)
    """
    # Sort by arrival_time within each group
    # (route_id, direction_id, stop_id)
    gtfs_stops = gtfs_stops.sort(
        ["route_id", "direction_id", "stop_id", "arrival_time"]
    )

    # Calculate headway as difference between consecutive arrivals at same stop
    gtfs_stops = gtfs_stops.with_columns(
        [
            pl.col("arrival_time")
            .diff()
            .over(["route_id", "direction_id", "stop_id"])
            .dt.total_seconds()
            .alias("scheduled_headway")
        ]
    )

    # Replace null (first occurrence at each stop) with None
    # to match original behavior
    gtfs_stops = gtfs_stops.with_columns(
        [
            pl.when(pl.col("scheduled_headway").is_null())
            .then(pl.lit(None))
            .otherwise(pl.col("scheduled_headway").cast(pl.Int64))
            .alias("scheduled_headway")
        ]
    )

    return gtfs_stops


def calculate_scheduled_travel_time(gtfs_stops: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate scheduled travel time from the start of each trip.

    Scheduled travel time is the elapsed time from the beginning of the trip
    to each stop.

    Methodology from gobbe/src/gtfs.py:371-373:
    - For each trip, find the minimum arrival time (trip start time)
    - Calculate difference between each stop's arrival time and trip start
    - Result is in seconds

    Args:
        gtfs_stops: DataFrame with GTFS stop times

    Returns:
        DataFrame with additional 'scheduled_tt' column (in seconds)
    """
    # Calculate trip start time (minimum arrival time for each trip)
    gtfs_stops = gtfs_stops.with_columns(
        [pl.col("arrival_time").min().over("trip_id").alias("trip_start_time")]
    )

    # Calculate scheduled travel time as difference from trip start
    gtfs_stops = gtfs_stops.with_columns(
        [
            (pl.col("arrival_time") - pl.col("trip_start_time"))
            .dt.total_seconds()
            .cast(pl.Int64)
            .alias("scheduled_tt")
        ]
    )

    # Drop the temporary trip_start_time column
    gtfs_stops = gtfs_stops.drop("trip_start_time")

    return gtfs_stops


def calculate_gtfs_metrics(gtfs_dir: str) -> pl.DataFrame:
    """
    Main function to calculate both scheduled headway and scheduled travel time.

    Args:
        gtfs_dir: Path to directory containing GTFS files

    Returns:
        DataFrame with GTFS stop times enriched with:
        - scheduled_headway: Time gap between consecutive vehicles at
          same stop (seconds)
        - scheduled_tt: Elapsed time from trip start to each stop
          (seconds)
    """
    # Load GTFS data
    gtfs_stops = load_gtfs_stop_times(gtfs_dir)

    # Calculate scheduled headway
    gtfs_stops = calculate_scheduled_headway(gtfs_stops)

    # Calculate scheduled travel time
    gtfs_stops = calculate_scheduled_travel_time(gtfs_stops)

    return gtfs_stops


def generate_direction_lookup(GTFS_DIR: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Generate lookup DataFrames from GTFS data with primary and secondary keys.

    Args:
        GTFS_DIR: Path to extracted GTFS directory

    Returns:
        Tuple of two polars DataFrames:
        - Primary: DataFrame with columns [trip_short_name, direction_id]
        - Secondary: DataFrame with columns [headsign_stop_id, direction_id]
    """
    gtfs_path = Path(GTFS_DIR)

    # Read trips.txt to get trip_short_name -> direction_id mapping
    trips_df = pl.read_csv(gtfs_path / "trips.txt")

    # Read stop_times.txt to get the last stop for each trip
    stop_times_df = pl.read_csv(gtfs_path / "stop_times.txt")

    # Primary lookup: trip_short_name -> direction_id
    primary_df = trips_df.select(["trip_short_name", "direction_id"])
    primary_df = primary_df.unique(subset=["trip_short_name"], keep="first")

    # Get the last stop_id for each trip (the headsign destination)
    last_stops = (
        stop_times_df.sort("stop_sequence")
        .group_by("trip_id")
        .agg([pl.col("stop_id").last().alias("headsign_stop_id")])
    )

    # Join trips with last stops to get headsign_stop_id with direction_id
    trips_with_stops = trips_df.join(last_stops, on="trip_id", how="left")

    # Secondary lookup: headsign_stop_id -> direction_id
    secondary_df = trips_with_stops.select(["headsign_stop_id", "direction_id"])
    secondary_df = secondary_df.filter(pl.col("headsign_stop_id").is_not_null())
    secondary_df = secondary_df.unique(subset=["headsign_stop_id"], keep="first")

    return primary_df, secondary_df


def generate_direction_on_custom_headsign(
    df: pl.DataFrame, lookup: dict[str, int], headsign_code_col: str = "destCode"
) -> pl.DataFrame:
    """
    Generate Direction IDs based on headsign and lookup. Meant for providers1
    that don't include direction_id in GTFS bundle (i.e Brightline).
    Example Mapping:
    headsign_to_dir = {
    "North Station": 0,
    "South Station": 1,
    "Forest Hills": 1,
    "Oak Grove": 0
    }
    """
    new_df = df.with_columns(
        pl.col(headsign_code_col).replace(lookup).alias("direction_id")
    )
    return new_df
