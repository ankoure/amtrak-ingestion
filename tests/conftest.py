import pytest
import sys
from pathlib import Path
from unittest.mock import MagicMock
import tempfile
import shutil

# Add amtraker_ingestion to Python path so chalicelib imports work
amtraker_path = Path(__file__).parent.parent / "amtraker_ingestion"
sys.path.insert(0, str(amtraker_path))


@pytest.fixture
def mock_s3_client():
    """Mock boto3 S3 client for testing."""
    client = MagicMock()
    return client


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path)


@pytest.fixture
def mock_gtfs_dir(temp_dir):
    """Create a mock GTFS directory structure with sample files."""
    gtfs_path = Path(temp_dir) / "gtfs"
    gtfs_path.mkdir()

    # Create sample GTFS files
    (gtfs_path / "stops.txt").write_text(
        "stop_id,stop_name,stop_lat,stop_lon\n"
        "BOS,Boston South Station,42.352271,-71.055242\n"
        "NYP,New York Penn Station,40.750568,-73.993519\n"
    )

    (gtfs_path / "routes.txt").write_text(
        "route_id,route_short_name,route_long_name,route_type\n"
        "NEC,NEC,Northeast Corridor,2\n"
    )

    (gtfs_path / "trips.txt").write_text(
        "route_id,service_id,trip_id,trip_headsign,trip_short_name,direction_id\n"
        "NEC,WEEKDAY,trip1,New York,2150,0\n"
        "NEC,WEEKDAY,trip2,Boston,2151,1\n"
    )

    (gtfs_path / "stop_times.txt").write_text(
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
        "trip1,08:00:00,08:05:00,BOS,1\n"
        "trip1,12:00:00,12:05:00,NYP,2\n"
        "trip2,14:00:00,14:05:00,NYP,1\n"
        "trip2,18:00:00,18:05:00,BOS,2\n"
    )

    return str(gtfs_path)


@pytest.fixture
def sample_train_data():
    """Sample train data from Amtraker API."""
    return {
        "2150-15": [
            {
                "trainID": "2150-15",
                "lat": 42.352,
                "lon": -71.055,
                "trainNum": "2150",
                "trainNumRaw": "2150",
                "objectID": 123,
                "destCode": "NYP",
                "originCode": "BOS",
                "heading": "S",
                "eventCode": "Enroute",
                "velocity": 80.5,
                "stations": [
                    {
                        "code": "BOS",
                        "tz": "America/New_York",
                        "bus": False,
                        "schArr": None,
                        "schDep": "2025-01-15T08:05:00",
                        "arr": None,
                        "dep": "2025-01-15T08:10:00",
                    },
                    {
                        "code": "NYP",
                        "tz": "America/New_York",
                        "bus": False,
                        "schArr": "2025-01-15T12:00:00",
                        "schDep": None,
                        "arr": "2025-01-15T12:15:00",
                        "dep": None,
                    },
                ],
            }
        ]
    }


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set up mock environment variables for testing."""
    monkeypatch.setenv("AWS_EXECUTION_ENV", "")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture(autouse=True)
def reset_logging():
    """Reset logging configuration between tests."""
    import logging

    logging.getLogger().handlers.clear()
