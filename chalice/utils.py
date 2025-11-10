from urllib.request import urlretrieve
from zipfile import ZipFile
from models.amtraker import TrainResponse


def get_latest_gtfs_archive(GTFS_URL: str):
    gtfs_zip, _ = urlretrieve(GTFS_URL, "gtfs.zip")
    # loading the temp.zip and creating a zip object
    with ZipFile(gtfs_zip, "r") as zObject:
        # Extracting all the members of the zip
        # into a specific location.
        zObject.extractall(path="./gtfs")
    return "./gtfs"


def trains_to_list(train_response: TrainResponse) -> list[dict]:
    """Convert TrainResponse to a list of Train dictionaries."""
    train_list: list[dict] = []
    for trains in train_response.root.values():
        for train in trains:
            train_list.append(train.model_dump())
    return train_list
