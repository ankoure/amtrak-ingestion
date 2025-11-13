from urllib.request import urlretrieve
from zipfile import ZipFile
from models.amtraker import TrainResponse
from tempfile import mkdtemp
import os
import polars as pl


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
