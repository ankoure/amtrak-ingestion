import pytest
from zoneinfo import ZoneInfo

from amtraker_ingestion.chalicelib.constants import (
    Provider,
    AMTRAKER_API,
    AMTRAK_STATIC_GTFS,
    BRIGHTLINE_STATIC_GTFS,
    VIA_RAIL_STATIC_GTFS,
    EASTERN_TIME,
    S3_BUCKET,
    CSV_FIELDS,
)


@pytest.mark.unit
class TestProvider:
    """Test Provider enum."""

    def test_provider_values(self):
        """Test that Provider enum has expected values."""
        assert Provider.AMTRAK.value == "Amtrak"
        assert Provider.VIA.value == "VIA"
        assert Provider.BRIGHTLINE.value == "Brightline"

    def test_provider_string_conversion(self):
        """Test that Provider enum converts to string correctly."""
        assert str(Provider.AMTRAK) == "Amtrak"
        assert str(Provider.VIA) == "VIA"
        assert str(Provider.BRIGHTLINE) == "Brightline"

    def test_provider_from_string(self):
        """Test creating Provider from string."""
        assert Provider("Amtrak") == Provider.AMTRAK
        assert Provider("VIA") == Provider.VIA
        assert Provider("Brightline") == Provider.BRIGHTLINE

    def test_provider_invalid_string_raises(self):
        """Test that invalid provider string raises ValueError."""
        with pytest.raises(ValueError):
            Provider("InvalidProvider")


@pytest.mark.unit
class TestConstants:
    """Test constant values."""

    def test_api_urls_are_strings(self):
        """Test that API URLs are valid strings."""
        assert isinstance(AMTRAKER_API, str)
        assert AMTRAKER_API.startswith("http")

    def test_gtfs_urls(self):
        """Test GTFS URLs are valid."""
        assert isinstance(AMTRAK_STATIC_GTFS, str)
        assert AMTRAK_STATIC_GTFS.startswith("http")

        assert isinstance(BRIGHTLINE_STATIC_GTFS, str)
        assert BRIGHTLINE_STATIC_GTFS.startswith("http")

        assert isinstance(VIA_RAIL_STATIC_GTFS, str)
        assert VIA_RAIL_STATIC_GTFS.startswith("http")

    def test_eastern_timezone(self):
        """Test Eastern timezone is configured."""
        assert isinstance(EASTERN_TIME, ZoneInfo)
        assert str(EASTERN_TIME) == "US/Eastern"

    def test_s3_bucket_name(self):
        """Test S3 bucket name is set."""
        assert isinstance(S3_BUCKET, str)
        assert len(S3_BUCKET) > 0

    def test_csv_fields(self):
        """Test CSV fields list."""
        assert isinstance(CSV_FIELDS, list)
        assert len(CSV_FIELDS) > 0
        assert "service_date" in CSV_FIELDS
        assert "route_id" in CSV_FIELDS
        assert "trip_id" in CSV_FIELDS
        assert "stop_id" in CSV_FIELDS
        assert "event_type" in CSV_FIELDS
        assert "event_time" in CSV_FIELDS
