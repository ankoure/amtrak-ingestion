"""
Data Transformation and Enrichment Module
==========================================

This module enriches raw Amtraker data with GTFS schedule information,
including direction IDs, scheduled headways, and travel times.

Main Functions
--------------
add_direction_id
    Adds GTFS direction IDs using primary and secondary lookup strategies
add_scheduled_metrics
    Adds scheduled headway and travel time from GTFS data
"""

import polars as pl
import time

from chalicelib.gtfs import generate_direction_lookup, calculate_gtfs_metrics
from chalicelib.config import get_logger, lambda_metric, get_dd_tags

logger = get_logger(__name__)


def add_direction_id(
    amtraker_df: pl.DataFrame, gtfs_dir: str, provider: str = "unknown"
) -> pl.DataFrame:
    """
    Enrich Amtraker train data with GTFS direction IDs.

    Uses a two-stage lookup strategy:

    1. **Primary lookup**: Match ``trainNumRaw`` to GTFS ``trip_short_name``
    2. **Secondary lookup**: Match ``destCode`` to GTFS headsign stop ID

    Parameters
    ----------
    amtraker_df : polars.DataFrame
        DataFrame with train data, must contain ``trainNumRaw`` and
        ``destCode`` columns.
    gtfs_dir : str
        Path to extracted GTFS directory containing trips.txt and
        stop_times.txt.

    Returns
    -------
    polars.DataFrame
        Input DataFrame enriched with:

        - ``direction_id``: GTFS direction (0 or 1), or None if not found
        - ``lookup_method``: "primary", "secondary", or None

    See Also
    --------
    generate_direction_lookup : Generates the lookup DataFrames from GTFS
    """
    start_time = time.time()
    input_rows = len(amtraker_df)
    if input_rows == 0:
        logger.info("No rows to enrich with direction_id")
        return amtraker_df
    logger.debug(
        f"Adding direction_id to {input_rows} rows using GTFS: {gtfs_dir}"
    )

    # Generate GTFS lookups (primary and secondary DataFrames)
    primary_lookup, secondary_lookup = generate_direction_lookup(gtfs_dir)

    # Primary lookup: join on trainNumRaw -> trip_short_name
    # Cast trip_short_name to string to match trainNumRaw data type
    df_with_primary = amtraker_df.join(
        primary_lookup.select(
            [
                pl.col("trip_short_name").cast(pl.Utf8).alias("trainNumRaw"),
                pl.col("direction_id").alias("direction_id_primary"),
            ]
        ),
        on="trainNumRaw",
        how="left",
    )

    # Secondary lookup: join on destCode (destination station)
    # -> headsign_stop_id
    df_with_both = df_with_primary.join(
        secondary_lookup.select(
            [
                pl.col("headsign_stop_id").cast(pl.Utf8).alias("destCode"),
                pl.col("direction_id").alias("direction_id_secondary"),
            ]
        ),
        on="destCode",
        how="left",
    )

    # Coalesce primary and secondary lookups, preferring primary
    df_enriched = df_with_both.with_columns(
        [
            pl.when(pl.col("direction_id_primary").is_not_null())
            .then(pl.col("direction_id_primary"))
            .when(pl.col("direction_id_secondary").is_not_null())
            .then(pl.col("direction_id_secondary"))
            .otherwise(None)
            .alias("direction_id"),
            pl.when(pl.col("direction_id_primary").is_not_null())
            .then(pl.lit("primary"))
            .when(pl.col("direction_id_secondary").is_not_null())
            .then(pl.lit("secondary"))
            .otherwise(None)
            .alias("lookup_method"),
        ]
    )

    # Calculate lookup statistics
    primary_hits = df_enriched.filter(
        pl.col("lookup_method") == "primary"
    ).height
    secondary_hits = df_enriched.filter(
        pl.col("lookup_method") == "secondary"
    ).height
    misses = df_enriched.filter(pl.col("direction_id").is_null()).height

    # Drop temporary columns
    df_enriched = df_enriched.drop(
        ["direction_id_primary", "direction_id_secondary"]
    )

    match_rate = 100 * (primary_hits + secondary_hits) / input_rows
    duration = time.time() - start_time
    logger.info(
        f"Direction ID lookup completed in {duration:.2f}s - "
        f"Primary: {primary_hits}, Secondary: {secondary_hits}, "
        f"Misses: {misses} "
        f"({match_rate:.1f}% match rate)"
    )

    tags = get_dd_tags(provider=provider)
    lambda_metric(
        "pipeline.gtfs.direction_id_match_rate_pct", match_rate, tags=tags
    )
    lambda_metric(
        "pipeline.gtfs.direction_id_primary_hits", primary_hits, tags=tags
    )
    lambda_metric(
        "pipeline.gtfs.direction_id_secondary_hits",
        secondary_hits,
        tags=tags,
    )
    lambda_metric("pipeline.gtfs.direction_id_misses", misses, tags=tags)
    lambda_metric(
        "pipeline.gtfs.enrichment_duration_seconds", duration, tags=tags
    )

    return df_enriched


def add_scheduled_metrics(
    amtraker_df: pl.DataFrame, gtfs_dir: str
) -> pl.DataFrame:
    """
    Add scheduled headway and travel time metrics from GTFS.

    Enriches train data with schedule-based metrics calculated from GTFS:

    - **Scheduled headway**: Expected time between consecutive vehicles
      at the same stop
    - **Scheduled travel time**: Expected elapsed time from trip start
      to each stop

    Parameters
    ----------
    amtraker_df : polars.DataFrame
        DataFrame with train data. Must have ``code`` (stop_id) and
        ``direction_id`` columns.
    gtfs_dir : str
        Path to extracted GTFS directory.

    Returns
    -------
    polars.DataFrame
        Input DataFrame enriched with:

        - ``scheduled_headway``: Headway in seconds (integer)
        - ``scheduled_tt``: Travel time in seconds (integer)

    Notes
    -----
    Metrics are aggregated (averaged) across all trips at each
    stop/direction combination to avoid cartesian product issues.

    See Also
    --------
    calculate_gtfs_metrics : Calculates raw GTFS metrics
    """
    start_time = time.time()
    input_rows = len(amtraker_df)
    if input_rows == 0:
        logger.info("No rows to enrich with scheduled metrics")
        return amtraker_df
    logger.debug(
        f"Adding scheduled metrics to {input_rows} rows using GTFS: {gtfs_dir}"
    )

    # Calculate GTFS metrics (includes scheduled_headway and scheduled_tt)
    gtfs_metrics = calculate_gtfs_metrics(gtfs_dir)
    logger.debug(f"Calculated GTFS metrics: {len(gtfs_metrics)} rows")

    # Aggregate GTFS metrics by stop and direction to get average values
    # This prevents cartesian product from multiple trips
    gtfs_aggregated = gtfs_metrics.group_by(["stop_id", "direction_id"]).agg(
        [
            pl.col("scheduled_headway").mean().alias("scheduled_headway"),
            pl.col("scheduled_tt").mean().alias("scheduled_tt"),
        ]
    )
    logger.debug(f"Aggregated GTFS metrics: {len(gtfs_aggregated)} rows")

    # Join on stop_id (GTFS) -> code (Amtraker) and direction_id
    enriched_df = amtraker_df.join(
        gtfs_aggregated.select(
            [
                pl.col("stop_id").cast(pl.Utf8).alias("code"),
                "direction_id",
                "scheduled_headway",
                "scheduled_tt",
            ]
        ),
        on=["code", "direction_id"],
        how="left",
    )

    # Calculate join success rate
    metrics_found = enriched_df.filter(
        pl.col("scheduled_headway").is_not_null()
    ).height
    metrics_missing = enriched_df.filter(
        pl.col("scheduled_headway").is_null()
    ).height

    # Convert scheduled metrics to integers (rounding from mean aggregation)
    enriched_df = enriched_df.with_columns(
        [
            pl.col("scheduled_headway").round(0).cast(pl.Int64),
            pl.col("scheduled_tt").round(0).cast(pl.Int64),
        ]
    )

    duration = time.time() - start_time
    logger.info(
        f"Scheduled metrics added in {duration:.2f}s - "
        f"Found: {metrics_found}, Missing: {metrics_missing} "
        f"({100 * metrics_found / input_rows:.1f}% match rate)"
    )

    return enriched_df
