import polars as pl

from gtfs import generate_direction_lookup, calculate_gtfs_metrics


def add_direction_id(amtraker_df: pl.DataFrame, gtfs_dir: str) -> pl.DataFrame:
    """
    Enrich Amtraker train data with GTFS information like direction_id.
    Uses primary lookup by trainNumRaw, falls back to secondary lookup by destCode (destination stop_id).

    Args:
        amtraker_df: Polars DataFrame with train data from read_amtraker_data
        gtfs_dir: Path to extracted GTFS directory

    Returns:
        Polars DataFrame enriched with direction_id and lookup_method columns
    """
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

    # Secondary lookup: join on destCode (destination station code) -> headsign_stop_id
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

    # Drop temporary columns
    df_enriched = df_enriched.drop(["direction_id_primary", "direction_id_secondary"])

    return df_enriched


def add_scheduled_metrics(amtraker_df: pl.DataFrame, gtfs_dir: str) -> pl.DataFrame:
    """
    Add scheduled headway and travel time metrics from GTFS to Amtraker data.

    Args:
        amtraker_df: Polars DataFrame with train data (must have direction_id column)
        gtfs_dir: Path to extracted GTFS directory

    Returns:
        Polars DataFrame enriched with scheduled_headway and scheduled_tt columns
    """
    # Calculate GTFS metrics (includes scheduled_headway and scheduled_tt)
    gtfs_metrics = calculate_gtfs_metrics(gtfs_dir)

    # Aggregate GTFS metrics by stop and direction to get average values
    # This prevents cartesian product from multiple trips
    gtfs_aggregated = gtfs_metrics.group_by(["stop_id", "direction_id"]).agg(
        [
            pl.col("scheduled_headway").mean().alias("scheduled_headway"),
            pl.col("scheduled_tt").mean().alias("scheduled_tt"),
        ]
    )

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

    # Convert scheduled metrics to integers (rounding from mean aggregation)
    enriched_df = enriched_df.with_columns(
        [
            pl.col("scheduled_headway").round(0).cast(pl.Int64),
            pl.col("scheduled_tt").round(0).cast(pl.Int64),
        ]
    )

    return enriched_df
