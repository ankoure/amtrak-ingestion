"""
Tests for main.py functions.

These tests use mocking extensively since the main functions orchestrate
external API calls, S3 operations, and file I/O.
"""

import pytest
import polars as pl
import gzip
import json
from unittest.mock import patch, MagicMock
from datetime import datetime

from chalicelib.main import (
    generate_event_data,
    check_gtfs_bundle_loop,
    collate_amtraker_data_for_date,
    collate_amtraker_data,
)
from chalicelib.constants import Provider


# ============================================================================
# Tests for generate_event_data()
# ============================================================================


@pytest.mark.unit
@patch("chalicelib.main.set_last_processed")
@patch("chalicelib.main.cleanup_old_gtfs_temp_dirs")
@patch("chalicelib.main.write_amtraker_events")
@patch("chalicelib.main.add_service_dates")
@patch("chalicelib.main.add_scheduled_metrics")
@patch("chalicelib.main.add_direction_id")
@patch("chalicelib.main.get_latest_gtfs_archive")
@patch("chalicelib.main.get_latest_gtfs_archive_from_cache")
@patch("chalicelib.main.read_amtraker_data")
@patch("chalicelib.main.ENVIRONMENT", "DEV")
@patch("chalicelib.main.AMTRAK_ENABLED", True)
@patch("chalicelib.main.VIA_ENABLED", False)
@patch("chalicelib.main.BRIGHTLINE_ENABLED", False)
def test_generate_event_data_amtrak_only(
    mock_read,
    mock_gtfs_cache,
    mock_gtfs_download,
    mock_add_direction,
    mock_add_scheduled,
    mock_add_service,
    mock_write,
    mock_cleanup,
    mock_set_last_processed,
):
    """Test generate_event_data with only Amtrak enabled."""
    # Setup mock data
    mock_amtrak_df = pl.DataFrame(
        {"trainID": ["2150-15"], "trainNum": ["2150"], "objectID": [123]}
    )
    mock_via_df = pl.DataFrame()
    mock_brightline_df = pl.DataFrame()

    mock_read.return_value = (mock_amtrak_df, mock_via_df, mock_brightline_df)
    mock_gtfs_cache.return_value = "/tmp/gtfs/amtrak"
    mock_add_direction.return_value = mock_amtrak_df
    mock_add_scheduled.return_value = mock_amtrak_df
    mock_add_service.return_value = mock_amtrak_df

    # Execute
    generate_event_data()

    # Verify
    mock_read.assert_called_once()
    mock_cleanup.assert_called()  # Called at start and end
    assert mock_cleanup.call_count == 2

    # Verify GTFS cache was used (not downloaded)
    mock_gtfs_cache.assert_called_once_with("Amtrak")
    mock_gtfs_download.assert_not_called()

    # Verify enrichment pipeline
    mock_add_direction.assert_called_once()
    mock_add_scheduled.assert_called_once()
    mock_add_service.assert_called_once()

    # Verify write was called with enriched data
    mock_write.assert_called_once()

    # Verify set_last_processed was NOT called (not PROD environment)
    mock_set_last_processed.assert_not_called()


@pytest.mark.unit
@patch("chalicelib.main.set_last_processed")
@patch("chalicelib.main.cleanup_old_gtfs_temp_dirs")
@patch("chalicelib.main.write_amtraker_events")
@patch("chalicelib.main.add_service_dates")
@patch("chalicelib.main.add_scheduled_metrics")
@patch("chalicelib.main.add_direction_id")
@patch("chalicelib.main.get_latest_gtfs_archive")
@patch("chalicelib.main.get_latest_gtfs_archive_from_cache")
@patch("chalicelib.main.read_amtraker_data")
@patch("chalicelib.main.ENVIRONMENT", "PROD")
@patch("chalicelib.main.AMTRAK_ENABLED", True)
@patch("chalicelib.main.VIA_ENABLED", False)
@patch("chalicelib.main.BRIGHTLINE_ENABLED", False)
def test_generate_event_data_prod_environment(
    mock_read,
    mock_gtfs_cache,
    mock_gtfs_download,
    mock_add_direction,
    mock_add_scheduled,
    mock_add_service,
    mock_write,
    mock_cleanup,
    mock_set_last_processed,
):
    """Test that set_last_processed is called in PROD environment."""
    # Setup mock data
    mock_amtrak_df = pl.DataFrame({"trainID": ["2150-15"]})
    mock_via_df = pl.DataFrame()
    mock_brightline_df = pl.DataFrame()

    mock_read.return_value = (mock_amtrak_df, mock_via_df, mock_brightline_df)
    mock_gtfs_cache.return_value = "/tmp/gtfs/amtrak"
    mock_add_direction.return_value = mock_amtrak_df
    mock_add_scheduled.return_value = mock_amtrak_df
    mock_add_service.return_value = mock_amtrak_df

    # Execute
    generate_event_data()

    # Verify set_last_processed WAS called in PROD
    mock_set_last_processed.assert_called_once()


@pytest.mark.unit
@patch("chalicelib.main.cleanup_old_gtfs_temp_dirs")
@patch("chalicelib.main.write_amtraker_events")
@patch("chalicelib.main.add_service_dates")
@patch("chalicelib.main.add_scheduled_metrics")
@patch("chalicelib.main.add_direction_id")
@patch("chalicelib.main.get_latest_gtfs_archive")
@patch("chalicelib.main.get_latest_gtfs_archive_from_cache")
@patch("chalicelib.main.read_amtraker_data")
@patch("chalicelib.main.ENVIRONMENT", "DEV")
@patch("chalicelib.main.AMTRAK_ENABLED", False)
@patch("chalicelib.main.VIA_ENABLED", False)
@patch("chalicelib.main.BRIGHTLINE_ENABLED", False)
def test_generate_event_data_no_providers_enabled(
    mock_read,
    mock_gtfs_cache,
    mock_gtfs_download,
    mock_add_direction,
    mock_add_scheduled,
    mock_add_service,
    mock_write,
    mock_cleanup,
):
    """Test generate_event_data with no providers enabled."""
    # Setup mock data
    mock_amtrak_df = pl.DataFrame()
    mock_via_df = pl.DataFrame()
    mock_brightline_df = pl.DataFrame()

    mock_read.return_value = (mock_amtrak_df, mock_via_df, mock_brightline_df)

    # Execute
    generate_event_data()

    # Verify
    mock_read.assert_called_once()
    mock_cleanup.assert_called()

    # Verify no processing occurred
    mock_gtfs_cache.assert_not_called()
    mock_add_direction.assert_not_called()
    mock_write.assert_not_called()


@pytest.mark.unit
@patch("chalicelib.main.cleanup_old_gtfs_temp_dirs")
@patch("chalicelib.main.write_amtraker_events")
@patch("chalicelib.main.add_service_dates")
@patch("chalicelib.main.add_scheduled_metrics")
@patch("chalicelib.main.add_direction_id")
@patch("chalicelib.main.get_latest_gtfs_archive")
@patch("chalicelib.main.get_latest_gtfs_archive_from_cache")
@patch("chalicelib.main.read_amtraker_data")
@patch("chalicelib.main.ENVIRONMENT", "DEV")
@patch("chalicelib.main.AMTRAK_ENABLED", True)
@patch("chalicelib.main.VIA_ENABLED", False)
@patch("chalicelib.main.BRIGHTLINE_ENABLED", False)
def test_generate_event_data_gtfs_cache_miss(
    mock_read,
    mock_gtfs_cache,
    mock_gtfs_download,
    mock_add_direction,
    mock_add_scheduled,
    mock_add_service,
    mock_write,
    mock_cleanup,
):
    """Test generate_event_data when GTFS cache miss occurs."""
    # Setup mock data
    mock_amtrak_df = pl.DataFrame({"trainID": ["2150-15"]})
    mock_via_df = pl.DataFrame()
    mock_brightline_df = pl.DataFrame()

    mock_read.return_value = (mock_amtrak_df, mock_via_df, mock_brightline_df)
    # Cache miss - returns None
    mock_gtfs_cache.return_value = None
    mock_gtfs_download.return_value = "/tmp/gtfs/downloaded"
    mock_add_direction.return_value = mock_amtrak_df
    mock_add_scheduled.return_value = mock_amtrak_df
    mock_add_service.return_value = mock_amtrak_df

    # Execute
    generate_event_data()

    # Verify fallback to download
    mock_gtfs_cache.assert_called_once_with("Amtrak")
    mock_gtfs_download.assert_called_once()


@pytest.mark.unit
@patch("chalicelib.main.cleanup_old_gtfs_temp_dirs")
@patch("chalicelib.main.write_amtraker_events")
@patch("chalicelib.main.add_service_dates")
@patch("chalicelib.main.add_scheduled_metrics")
@patch("chalicelib.main.generate_direction_on_custom_headsign")
@patch("chalicelib.main.add_direction_id")
@patch("chalicelib.main.get_latest_gtfs_archive")
@patch("chalicelib.main.get_latest_gtfs_archive_from_cache")
@patch("chalicelib.main.read_amtraker_data")
@patch("chalicelib.main.ENVIRONMENT", "DEV")
@patch("chalicelib.main.AMTRAK_ENABLED", True)
@patch("chalicelib.main.VIA_ENABLED", True)
@patch("chalicelib.main.BRIGHTLINE_ENABLED", True)
def test_generate_event_data_all_providers(
    mock_read,
    mock_gtfs_cache,
    mock_gtfs_download,
    mock_add_direction,
    mock_generate_direction,
    mock_add_scheduled,
    mock_add_service,
    mock_write,
    mock_cleanup,
):
    """Test generate_event_data with all providers enabled."""
    # Setup mock data
    mock_amtrak_df = pl.DataFrame({"trainID": ["2150-15"]})
    mock_via_df = pl.DataFrame({"trainID": ["via-1"]})
    mock_brightline_df = pl.DataFrame({"trainID": ["bl-1"]})

    mock_read.return_value = (mock_amtrak_df, mock_via_df, mock_brightline_df)
    mock_gtfs_cache.return_value = "/tmp/gtfs"
    mock_add_direction.return_value = mock_amtrak_df
    mock_generate_direction.return_value = mock_brightline_df
    mock_add_scheduled.return_value = mock_amtrak_df
    mock_add_service.return_value = mock_amtrak_df

    # Execute
    generate_event_data()

    # Verify all three providers were processed
    assert mock_gtfs_cache.call_count == 3
    mock_gtfs_cache.assert_any_call("Amtrak")
    mock_gtfs_cache.assert_any_call("Via")
    mock_gtfs_cache.assert_any_call("Brightline")

    # Verify write was called 3 times (once per provider)
    assert mock_write.call_count == 3


@pytest.mark.unit
@patch("chalicelib.main.cleanup_old_gtfs_temp_dirs")
@patch("chalicelib.main.read_amtraker_data")
@patch("chalicelib.main.AMTRAK_ENABLED", True)
@patch("chalicelib.main.VIA_ENABLED", False)
@patch("chalicelib.main.BRIGHTLINE_ENABLED", False)
def test_generate_event_data_error_handling(mock_read, mock_cleanup):
    """Test that errors are properly raised and logged."""
    # Setup mock to raise an exception
    mock_read.side_effect = Exception("API connection failed")

    # Execute and verify exception is raised
    with pytest.raises(Exception, match="API connection failed"):
        generate_event_data()

    # Verify cleanup was still attempted at start
    mock_cleanup.assert_called()


# ============================================================================
# Tests for check_gtfs_bundle_loop()
# ============================================================================


@pytest.mark.unit
@patch("chalicelib.main.set_s3_json")
@patch("chalicelib.main.upload_gtfs_bundle")
@patch("chalicelib.main.urllib.request.urlretrieve")
@patch("chalicelib.main.get_gtfs_last_modified")
@patch("chalicelib.main.get_s3_json")
def test_check_gtfs_bundle_loop_all_up_to_date(
    mock_get_s3_json,
    mock_get_last_modified,
    mock_urlretrieve,
    mock_upload_gtfs,
    mock_set_s3_json,
):
    """Test check_gtfs_bundle_loop when all bundles are up-to-date."""
    # Setup mock cache data
    test_date = datetime(2025, 1, 15, 10, 30, 0)
    mock_cache = {
        "Amtrak": {"last_modified": test_date.isoformat()},
        "Via": {"last_modified": test_date.isoformat()},
        "Brightline": {"last_modified": test_date.isoformat()},
    }
    mock_get_s3_json.return_value = mock_cache
    mock_get_last_modified.return_value = test_date

    # Execute
    check_gtfs_bundle_loop()

    # Verify early exit - no downloads occurred
    mock_urlretrieve.assert_not_called()
    mock_upload_gtfs.assert_not_called()
    mock_set_s3_json.assert_not_called()


@pytest.mark.unit
@patch("chalicelib.main.set_s3_json")
@patch("chalicelib.main.upload_gtfs_bundle")
@patch("chalicelib.main.urllib.request.urlretrieve")
@patch("chalicelib.main.get_gtfs_last_modified")
@patch("chalicelib.main.get_s3_json")
def test_check_gtfs_bundle_loop_cache_empty(
    mock_get_s3_json,
    mock_get_last_modified,
    mock_urlretrieve,
    mock_upload_gtfs,
    mock_set_s3_json,
):
    """Test check_gtfs_bundle_loop when cache doesn't exist."""
    # Setup - cache doesn't exist
    mock_get_s3_json.side_effect = Exception("NoSuchKey")
    test_date = datetime(2025, 1, 15, 10, 30, 0)
    mock_get_last_modified.return_value = test_date
    mock_urlretrieve.return_value = ("/tmp/gtfs.zip", {})

    # Execute
    check_gtfs_bundle_loop()

    # Verify all three bundles were downloaded
    assert mock_urlretrieve.call_count == 3
    assert mock_upload_gtfs.call_count == 3

    # Verify cache was updated
    mock_set_s3_json.assert_called_once()
    updated_cache = mock_set_s3_json.call_args[0][0]
    assert "Amtrak" in updated_cache
    assert "Via" in updated_cache
    assert "Brightline" in updated_cache


@pytest.mark.unit
@patch("chalicelib.main.set_s3_json")
@patch("chalicelib.main.upload_gtfs_bundle")
@patch("chalicelib.main.urllib.request.urlretrieve")
@patch("chalicelib.main.get_gtfs_last_modified")
@patch("chalicelib.main.get_s3_json")
def test_check_gtfs_bundle_loop_one_bundle_updated(
    mock_get_s3_json,
    mock_get_last_modified,
    mock_urlretrieve,
    mock_upload_gtfs,
    mock_set_s3_json,
):
    """Test check_gtfs_bundle_loop when one bundle has been updated."""
    # Setup - Amtrak bundle is outdated
    old_date = datetime(2025, 1, 10, 10, 0, 0)
    new_date = datetime(2025, 1, 15, 10, 0, 0)

    mock_cache = {
        "Amtrak": {"last_modified": old_date.isoformat()},
        "Via": {"last_modified": new_date.isoformat()},
        "Brightline": {"last_modified": new_date.isoformat()},
    }
    mock_get_s3_json.return_value = mock_cache

    # Mock different dates for different bundles
    def get_last_modified_side_effect(url):
        if "amtrak" in url.lower():
            return new_date
        return new_date

    mock_get_last_modified.side_effect = get_last_modified_side_effect
    mock_urlretrieve.return_value = ("/tmp/gtfs.zip", {})

    # Execute
    check_gtfs_bundle_loop()

    # Verify only Amtrak was downloaded
    assert mock_urlretrieve.call_count == 1
    assert mock_upload_gtfs.call_count == 1

    # Verify cache was updated
    mock_set_s3_json.assert_called_once()


@pytest.mark.unit
@patch("chalicelib.main.upload_gtfs_bundle")
@patch("chalicelib.main.urllib.request.urlretrieve")
@patch("chalicelib.main.get_gtfs_last_modified")
@patch("chalicelib.main.get_s3_json")
def test_check_gtfs_bundle_loop_missing_last_modified(
    mock_get_s3_json,
    mock_get_last_modified,
    mock_urlretrieve,
    mock_upload_gtfs,
):
    """Test when get_gtfs_last_modified returns None."""
    mock_get_s3_json.return_value = {}
    # First call returns None (triggers all_up_to_date=False and processing)
    # Subsequent calls during download phase return valid dates
    mock_get_last_modified.side_effect = [
        None,  # First check for Amtrak in early exit check
        datetime(2025, 1, 15),  # Amtrak in download loop
        datetime(2025, 1, 15),  # Via in download loop
        datetime(2025, 1, 15),  # Brightline in download loop
    ]
    mock_urlretrieve.return_value = ("/tmp/gtfs.zip", {})

    # Execute - should not raise exception
    check_gtfs_bundle_loop()

    # Verify it handled the None gracefully and downloaded bundles
    assert mock_get_last_modified.called
    # Should have attempted downloads for bundles with valid last_modified
    assert mock_urlretrieve.call_count >= 1


# ============================================================================
# Tests for collate_amtraker_data_for_date()
# ============================================================================


@pytest.mark.unit
@patch("chalicelib.main.s3_client")
def test_collate_amtraker_data_for_date_success(mock_s3_client):
    """Test successful collation of data for a specific date."""
    # Setup mock S3 response
    sample_event = {
        "trainID": "2150-15",
        "trainNum": "2150",
        "timestamp": "2025-01-15",
    }
    compressed_data = gzip.compress(json.dumps([sample_event]).encode("utf-8"))

    mock_s3_client.list_objects_v2.return_value = {
        "Contents": [
            {
                "Key": "Events-live/data/raw/amtrak/Year=2025/"
                "Month=01/Day=15/data.json.gz"
            }
        ]
    }

    mock_s3_client.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=compressed_data))
    }

    # Execute
    result = collate_amtraker_data_for_date(2025, 1, 15, Provider.AMTRAK)

    # Verify
    assert len(result) == 1
    assert result[0]["trainID"] == "2150-15"
    mock_s3_client.list_objects_v2.assert_called_once()


@pytest.mark.unit
@patch("chalicelib.main.s3_client")
def test_collate_amtraker_data_for_date_no_files(mock_s3_client):
    """Test collation when no files exist for the date."""
    # Setup mock S3 response with no files
    mock_s3_client.list_objects_v2.return_value = {}

    # Execute
    result = collate_amtraker_data_for_date(2025, 1, 15, Provider.AMTRAK)

    # Verify
    assert result == []
    mock_s3_client.list_objects_v2.assert_called_once()


@pytest.mark.unit
@patch("chalicelib.main.s3_client")
def test_collate_amtraker_data_for_date_multiple_files(mock_s3_client):
    """Test collation with multiple JSON files."""
    # Setup mock S3 response with multiple files
    event1 = {"trainID": "2150-15", "trainNum": "2150"}
    event2 = {"trainID": "2151-15", "trainNum": "2151"}

    compressed1 = gzip.compress(json.dumps([event1]).encode("utf-8"))
    compressed2 = gzip.compress(json.dumps([event2]).encode("utf-8"))

    mock_s3_client.list_objects_v2.return_value = {
        "Contents": [
            {
                "Key": "Events-live/data/raw/amtrak/Year=2025/"
                "Month=01/Day=15/data1.json.gz"
            },
            {
                "Key": "Events-live/data/raw/amtrak/Year=2025/"
                "Month=01/Day=15/data2.json.gz"
            },
        ]
    }

    # Mock get_object to return different data for each call
    mock_s3_client.get_object.side_effect = [
        {"Body": MagicMock(read=MagicMock(return_value=compressed1))},
        {"Body": MagicMock(read=MagicMock(return_value=compressed2))},
    ]

    # Execute
    result = collate_amtraker_data_for_date(2025, 1, 15, Provider.AMTRAK)

    # Verify
    assert len(result) == 2
    assert result[0]["trainID"] == "2150-15"
    assert result[1]["trainID"] == "2151-15"


@pytest.mark.unit
@patch("chalicelib.main.s3_client")
def test_collate_amtraker_data_for_date_single_event(mock_s3_client):
    """Test collation when JSON file contains a single event (not list)."""
    # Setup - single event object instead of list
    single_event = {"trainID": "2150-15", "trainNum": "2150"}
    compressed_data = gzip.compress(json.dumps(single_event).encode("utf-8"))

    mock_s3_client.list_objects_v2.return_value = {
        "Contents": [
            {
                "Key": "Events-live/data/raw/amtrak/Year=2025/"
                "Month=01/Day=15/data.json.gz"
            }
        ]
    }

    mock_s3_client.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=compressed_data))
    }

    # Execute
    result = collate_amtraker_data_for_date(2025, 1, 15, Provider.AMTRAK)

    # Verify - single event is converted to list
    assert len(result) == 1
    assert result[0]["trainID"] == "2150-15"


@pytest.mark.unit
@patch("chalicelib.main.s3_client")
def test_collate_amtraker_data_for_date_via_provider(mock_s3_client):
    """Test collation with Via Rail provider."""
    sample_event = {"trainID": "via-1"}
    compressed_data = gzip.compress(json.dumps([sample_event]).encode("utf-8"))

    mock_s3_client.list_objects_v2.return_value = {
        "Contents": [
            {
                "Key": "Events-live/data/raw/via/Year=2025/Month=01/Day=15/data.json.gz"
            }
        ]
    }

    mock_s3_client.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=compressed_data))
    }

    # Execute
    collate_amtraker_data_for_date(2025, 1, 15, Provider.VIA)

    # Verify correct prefix was used
    call_args = mock_s3_client.list_objects_v2.call_args[1]
    assert "via" in call_args["Prefix"].lower()


@pytest.mark.unit
@patch("chalicelib.main.s3_client")
def test_collate_amtraker_data_for_date_error_handling(mock_s3_client):
    """Test error handling in collation."""
    # Setup mock to raise an exception
    mock_s3_client.list_objects_v2.side_effect = Exception(
        "S3 connection failed"
    )

    # Execute and verify exception is raised
    with pytest.raises(Exception, match="S3 connection failed"):
        collate_amtraker_data_for_date(2025, 1, 15, Provider.AMTRAK)


# ============================================================================
# Tests for collate_amtraker_data()
# ============================================================================


@pytest.mark.unit
@patch("chalicelib.main.glob.glob")
@patch("chalicelib.main._compress_and_upload_file")
@patch("chalicelib.main.write_event")
@patch("chalicelib.main.collate_amtraker_data_for_date")
@patch("chalicelib.main.datetime")
@patch("chalicelib.main.AMTRAK_ENABLED", True)
@patch("chalicelib.main.VIA_ENABLED", False)
@patch("chalicelib.main.BRIGHTLINE_ENABLED", False)
def test_collate_amtraker_data_amtrak_only(
    mock_datetime,
    mock_collate_for_date,
    mock_write_event,
    mock_compress_upload,
    mock_glob,
):
    """Test daily collation with only Amtrak enabled."""
    # Setup mock datetime to control "yesterday"
    mock_now = datetime(2025, 1, 16, 12, 0, 0)
    mock_datetime.now.return_value = mock_now

    # Setup mock data
    sample_events = [
        {"trainID": "2150-15", "trainNum": "2150"},
        {"trainID": "2151-15", "trainNum": "2151"},
    ]
    mock_collate_for_date.return_value = sample_events

    # Mock glob to return CSV files that match the provider pattern
    # The code checks for "daily-{provider_str}-data" in the file path
    mock_glob.return_value = [
        "/tmp/daily-Amtrak-data/2025/01/15/route1.csv",
        "/tmp/daily-Amtrak-data/2025/01/15/route2.csv",
    ]

    # Execute
    collate_amtraker_data()

    # Verify collate_for_date was called with yesterday's date
    mock_collate_for_date.assert_called_once_with(
        2025,
        1,
        15,
        Provider.AMTRAK,  # Yesterday
    )

    # Verify all events were written
    assert mock_write_event.call_count == 2

    # Verify files were uploaded
    assert mock_compress_upload.call_count == 2


@pytest.mark.unit
@patch("chalicelib.main.glob.glob")
@patch("chalicelib.main._compress_and_upload_file")
@patch("chalicelib.main.write_event")
@patch("chalicelib.main.collate_amtraker_data_for_date")
@patch("chalicelib.main.datetime")
@patch("chalicelib.main.AMTRAK_ENABLED", True)
@patch("chalicelib.main.VIA_ENABLED", True)
@patch("chalicelib.main.BRIGHTLINE_ENABLED", True)
def test_collate_amtraker_data_all_providers(
    mock_datetime,
    mock_collate_for_date,
    mock_write_event,
    mock_compress_upload,
    mock_glob,
):
    """Test daily collation with all providers enabled."""
    # Setup mock datetime
    mock_now = datetime(2025, 1, 16, 12, 0, 0)
    mock_datetime.now.return_value = mock_now

    # Setup mock data - different events for each provider
    mock_collate_for_date.side_effect = [
        [{"trainID": "amtrak-1"}],  # Amtrak
        [{"trainID": "via-1"}],  # Via
        [{"trainID": "bl-1"}],  # Brightline
    ]

    # Mock glob to return files for each provider
    mock_glob.return_value = [
        "/tmp/daily-amtrak-data/2025/01/15/route1.csv",
        "/tmp/daily-via-data/2025/01/15/route1.csv",
        "/tmp/daily-brightline-data/2025/01/15/route1.csv",
    ]

    # Execute
    collate_amtraker_data()

    # Verify all three providers were processed
    assert mock_collate_for_date.call_count == 3
    mock_collate_for_date.assert_any_call(2025, 1, 15, Provider.AMTRAK)
    mock_collate_for_date.assert_any_call(2025, 1, 15, Provider.VIA)
    mock_collate_for_date.assert_any_call(2025, 1, 15, Provider.BRIGHTLINE)


@pytest.mark.unit
@patch("chalicelib.main.glob.glob")
@patch("chalicelib.main._compress_and_upload_file")
@patch("chalicelib.main.write_event")
@patch("chalicelib.main.collate_amtraker_data_for_date")
@patch("chalicelib.main.datetime")
@patch("chalicelib.main.AMTRAK_ENABLED", True)
@patch("chalicelib.main.VIA_ENABLED", False)
@patch("chalicelib.main.BRIGHTLINE_ENABLED", False)
def test_collate_amtraker_data_no_events(
    mock_datetime,
    mock_collate_for_date,
    mock_write_event,
    mock_compress_upload,
    mock_glob,
):
    """Test daily collation when no events are found."""
    # Setup mock datetime
    mock_now = datetime(2025, 1, 16, 12, 0, 0)
    mock_datetime.now.return_value = mock_now

    # Setup mock to return no events
    mock_collate_for_date.return_value = []
    mock_glob.return_value = []

    # Execute
    collate_amtraker_data()

    # Verify no writes or uploads occurred
    mock_write_event.assert_not_called()
    mock_compress_upload.assert_not_called()


@pytest.mark.unit
@patch("chalicelib.main.glob.glob")
@patch("chalicelib.main._compress_and_upload_file")
@patch("chalicelib.main.write_event")
@patch("chalicelib.main.collate_amtraker_data_for_date")
@patch("chalicelib.main.datetime")
@patch("chalicelib.main.AMTRAK_ENABLED", True)
@patch("chalicelib.main.VIA_ENABLED", False)
@patch("chalicelib.main.BRIGHTLINE_ENABLED", False)
def test_collate_amtraker_data_error_continues(
    mock_datetime,
    mock_collate_for_date,
    mock_write_event,
    mock_compress_upload,
    mock_glob,
):
    """Test that errors in one provider don't stop processing of others."""
    # Setup mock datetime
    mock_now = datetime(2025, 1, 16, 12, 0, 0)
    mock_datetime.now.return_value = mock_now

    # Setup mock to raise exception
    mock_collate_for_date.side_effect = Exception("S3 error")
    mock_glob.return_value = []

    # Execute - should not raise exception (errors are logged but not raised)
    collate_amtraker_data()

    # Verify function completed despite error
    mock_collate_for_date.assert_called_once()
