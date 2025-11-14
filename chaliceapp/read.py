from chaliceapp.timefilter import filter_events
from chaliceapp.models.amtraker import TrainResponse
import requests
from pydantic import ValidationError
from chaliceapp.constants import AMTRAKER_API
import polars as pl


def validate_amtraker_data(amtraker_api_url: str) -> TrainResponse:
    """
    Fetch and validate train data from Amtraker API.

    Args:
        amtraker_api_url: URL to fetch train data from

    Returns:
        TrainResponse containing validated Train objects

    Raises:
        ValidationError: If the API response doesn't match the expected schema
        requests.RequestException: If the HTTP request fails
    """
    r = requests.get(amtraker_api_url)
    r.raise_for_status()
    data = r.json()

    try:
        # Validate the response data
        validated_data = TrainResponse.model_validate(data)

        return validated_data

    except ValidationError as e:
        print(f"Validation error while processing Amtraker data: {e}")
        raise


def trainresponse_to_polars(train_response: TrainResponse) -> pl.DataFrame:
    """
    Convert TrainResponse to Polars DataFrame.

    The TrainResponse is a dict mapping train numbers to lists of Train
    objects. This flattens it into a single DataFrame with one row per
    train instance.
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
    new_df = polars_df.filter(~pl.col("bus") & (pl.col("status") == "Departed"))
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
    train_response = validate_amtraker_data(AMTRAKER_API)
    df = trainresponse_to_polars(train_response)
    new_df = remove_excess_fields(df)
    exploded_df = explode_df(new_df)
    cleanup_stations = remove_excess_columns_from_stations(exploded_df)
    time_filter = filter_events(cleanup_stations, "dep")
    remove_bus_df = remove_bus(time_filter)
    return split_df_by_provider(remove_bus_df)


if __name__ == "__main__":
    amtrak_df, via_df, brightline_df = read_amtraker_data()
    print(amtrak_df)
    print(via_df)
    print(brightline_df)
