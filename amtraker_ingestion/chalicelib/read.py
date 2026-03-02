"""
Data Reading and Ingestion Module
==================================

This module handles fetching and transforming data from the Amtraker API
into Polars DataFrames for further processing.

The main entry point is :func:`read_amtraker_data`, which orchestrates
the complete data reading pipeline.

Pipeline Steps
--------------
1. Fetch and validate API response with Pydantic
2. Convert to Polars DataFrame
3. Remove excess fields and expand nested data
4. Filter by time and remove bus services
5. Split by transit provider
"""

from chalicelib.timefilter import filter_events
from chalicelib.models.amtraker import TrainResponse
from chalicelib.config import get_logger, lambda_metric, get_dd_tags
import requests
from pydantic import ValidationError
from chalicelib.constants import AMTRAKER_API
import polars as pl
import time

logger = get_logger(__name__)


def validate_amtraker_data(amtraker_api_url: str) -> TrainResponse:
    """
    Fetch and validate train data from Amtraker API.

    Makes an HTTP GET request to the Amtraker API and validates the response
    against the TrainResponse Pydantic model.

    Parameters
    ----------
    amtraker_api_url : str
        Full URL to fetch train data from.

    Returns
    -------
    TrainResponse
        Validated response containing Train objects organized by train number.

    Raises
    ------
    pydantic.ValidationError
        If the API response doesn't match the expected schema.
    requests.RequestException
        If the HTTP request fails (timeout, connection error, etc.).

    Examples
    --------
    >>> url = "https://api-v3.amtraker.com/v3/trains"
    >>> response = validate_amtraker_data(url)
    >>> len(response.root)  # Number of train numbers
    150
    """
    logger.debug(f"Fetching data from Amtraker API: {amtraker_api_url}")
    start_time = time.time()

    try:
        r = requests.get(amtraker_api_url, timeout=30)
        r.raise_for_status()
        request_duration = time.time() - start_time

        data = r.json()
        response_size = len(r.content)

        logger.info(
            f"API request successful - "
            f"{response_size} bytes received in {request_duration:.2f}s"
        )

        # Validate the response data
        validation_start = time.time()
        validated_data = TrainResponse.model_validate(data)
        validation_duration = time.time() - validation_start

        # Count trains in response
        train_count = sum(
            len(trains) for trains in validated_data.root.values()
        )

        logger.info(
            f"API response validated in {validation_duration:.2f}s - "
            f"{train_count} trains found"
        )

        tags = get_dd_tags()
        lambda_metric(
            "pipeline.api.request_duration_seconds",
            request_duration,
            tags=tags,
        )
        lambda_metric("pipeline.api.response_bytes", response_size, tags=tags)
        lambda_metric("pipeline.api.trains_fetched", train_count, tags=tags)

        return validated_data

    except requests.RequestException as e:
        logger.error(
            f"HTTP request failed after {time.time() - start_time:.2f}s: {e}",
            exc_info=True,
        )
        lambda_metric("pipeline.api.request_failures", 1, tags=get_dd_tags())
        raise
    except ValidationError as e:
        logger.error(
            f"Validation error while processing Amtraker data: {e}",
            exc_info=True,
        )
        lambda_metric("pipeline.api.validation_errors", 1, tags=get_dd_tags())
        raise


def trainresponse_to_polars(train_response: TrainResponse) -> pl.DataFrame:
    """
    Convert TrainResponse to Polars DataFrame.

    Flattens the hierarchical TrainResponse structure (train numbers mapping
    to lists of Train objects) into a single DataFrame with one row per
    train instance.

    Parameters
    ----------
    train_response : TrainResponse
        Validated API response with train data.

    Returns
    -------
    polars.DataFrame
        DataFrame with one row per train, containing all train fields
        including nested station data.
    """
    # Option 1: Using Pydantic's model_dump() and flattening
    trains_data = []
    for train_num, train_list in train_response.root.items():
        for train in train_list:
            # Convert each Train to a dict and add it to our list
            train_dict = train.model_dump(mode="python")
            trains_data.append(train_dict)

    # Create DataFrame from list of dicts
    df = pl.DataFrame(trains_data)
    return df


def remove_excess_fields(polars_df: pl.DataFrame) -> pl.DataFrame:
    new_df = polars_df.drop(
        [
            "trainNum",
            "trainID",
            "lat",
            "lon",
            "trainTimely",
            "iconColor",
            "heading",
            "eventCode",
            "eventTZ",
            "eventName",
            "origCode",
            "originTZ",
            "origName",
            "destTZ",
            "destName",
            "trainState",
            "velocity",
            "statusMsg",
            "createdAt",
            "updatedAt",
            "lastValTS",
            "objectID",
            "onlyOfTrainNum",
            "alerts",
        ]
    )
    return new_df


def explode_df(polars_df: pl.DataFrame) -> pl.DataFrame:
    df_exploded = polars_df.explode("stations")
    # Unnest the stations struct to flatten it into individual columns
    df_exploded = df_exploded.unnest("stations")

    # Convert timestamp strings to datetime objects
    # Replace 'Z' with '+00:00' to handle ISO 8601 UTC format (used by Via Rail)
    df_exploded = df_exploded.with_columns(
        [
            pl.col("arr")
            .str.replace("Z", "+00:00")
            .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%z", strict=False),
            pl.col("dep")
            .str.replace("Z", "+00:00")
            .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%z", strict=False),
            pl.col("schArr")
            .str.replace("Z", "+00:00")
            .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%z", strict=False),
            pl.col("schDep")
            .str.replace("Z", "+00:00")
            .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%z", strict=False),
        ]
    )

    return df_exploded


def remove_excess_columns_from_stations(
    polars_df: pl.DataFrame,
) -> pl.DataFrame:
    new_df = polars_df.drop(
        [
            "name",
            "arrCmnt",
            "depCmnt",
            "platform",
            "providerShort",
        ]
    )
    return new_df


def remove_bus(polars_df: pl.DataFrame) -> pl.DataFrame:
    # Filter to only include rows where bus is False
    new_df = polars_df.filter(
        ~pl.col("bus") & (pl.col("status") == "Departed")
    )
    return new_df


def split_df_by_provider(
    polars_df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    # Split into separate dataframes
    amtrak_df = polars_df.filter(pl.col("provider") == "Amtrak")
    via_df = polars_df.filter(pl.col("provider") == "Via")
    brightline_df = polars_df.filter(pl.col("provider") == "Brightline")

    return amtrak_df, via_df, brightline_df


def read_amtraker_data() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Read and process Amtraker API data through complete pipeline.

    Returns:
        Tuple of (amtrak_df, via_df, brightline_df) DataFrames
    """
    logger.debug("Starting Amtraker data processing pipeline")
    pipeline_start = time.time()

    # Fetch and validate
    train_response = validate_amtraker_data(AMTRAKER_API)

    # Convert to DataFrame
    df = trainresponse_to_polars(train_response)
    logger.debug(f"Converted to Polars DataFrame: {len(df)} rows")

    # Process through pipeline
    new_df = remove_excess_fields(df)
    logger.debug(f"Removed excess fields: {len(new_df)} rows")

    exploded_df = explode_df(new_df)
    logger.debug(f"Exploded stations: {len(exploded_df)} rows")

    cleanup_stations = remove_excess_columns_from_stations(exploded_df)
    logger.debug(f"Cleaned up station columns: {len(cleanup_stations)} rows")

    time_filter = filter_events(cleanup_stations, "dep")
    logger.debug(f"Applied time filter: {len(time_filter)} rows")

    remove_bus_df = remove_bus(time_filter)
    logger.debug(
        f"Removed bus services and non-departed: {len(remove_bus_df)} rows"
    )

    # Split by provider
    amtrak_df, via_df, brightline_df = split_df_by_provider(remove_bus_df)

    pipeline_duration = time.time() - pipeline_start
    logger.info(
        f"Data processing pipeline completed in {pipeline_duration:.2f}s - "
        f"Input: {len(df)} rows, Output: {len(remove_bus_df)} rows "
        f"(Amtrak: {len(amtrak_df)}, Via: {len(via_df)}, "
        f"Brightline: {len(brightline_df)})"
    )

    tags = get_dd_tags()
    lambda_metric(
        "pipeline.read.rows_after_time_filter", len(time_filter), tags=tags
    )
    lambda_metric(
        "pipeline.read.rows_after_bus_removal",
        len(remove_bus_df),
        tags=tags,
    )

    return amtrak_df, via_df, brightline_df


if __name__ == "__main__":
    amtrak_df, via_df, brightline_df = read_amtraker_data()
    print(amtrak_df)
    print(via_df)
    print(brightline_df)
