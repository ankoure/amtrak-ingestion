import pytest
from unittest.mock import Mock, patch
import os
import tempfile
import shutil
from pathlib import Path
from zipfile import ZipFile
from botocore.exceptions import ClientError

from amtraker_ingestion.chalicelib.utils import (
    temp_gtfs_directory,
    cleanup_old_gtfs_temp_dirs,
    get_latest_gtfs_archive_from_cache,
    get_latest_gtfs_archive,
    trains_to_list,
)


@pytest.mark.unit
class TestTempGTFSDirectory:
    """Test the temp_gtfs_directory context manager."""

    def test_creates_temporary_directory(self):
        """Test that context manager creates a temporary directory."""
        with temp_gtfs_directory() as temp_dir:
            assert os.path.exists(temp_dir)
            assert os.path.isdir(temp_dir)
            assert "gtfs_" in temp_dir

    def test_cleans_up_directory_on_exit(self):
        """Test that directory is removed after context exits."""
        with temp_gtfs_directory() as temp_dir:
            temp_path = temp_dir

        assert not os.path.exists(temp_path)

    def test_cleans_up_directory_on_exception(self):
        """Test that directory is removed even if exception occurs."""
        temp_path = None
        try:
            with temp_gtfs_directory() as temp_dir:
                temp_path = temp_dir
                raise ValueError("Test exception")
        except ValueError:
            pass

        assert not os.path.exists(temp_path)


@pytest.mark.unit
class TestCleanupOldGTFSTempDirs:
    """Test cleanup_old_gtfs_temp_dirs function."""

    def test_cleanup_with_no_directories(self):
        """Test cleanup when no temp directories exist."""
        with patch("glob.glob", return_value=[]):
            cleanup_old_gtfs_temp_dirs()

    def test_cleanup_removes_old_directories(self):
        """Test cleanup removes existing temp directories."""
        # Create a few temporary directories
        temp_dirs = []
        for i in range(3):
            temp_dir = tempfile.mkdtemp(prefix="gtfs_")
            temp_dirs.append(temp_dir)

        try:
            with patch("glob.glob", return_value=temp_dirs):
                cleanup_old_gtfs_temp_dirs()

            # Verify all directories were removed
            for temp_dir in temp_dirs:
                assert not os.path.exists(temp_dir)
        finally:
            # Cleanup in case test fails
            for temp_dir in temp_dirs:
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)

    def test_cleanup_handles_removal_errors(self):
        """Test cleanup continues even if removal fails."""
        with patch(
            "glob.glob", return_value=["/tmp/gtfs_fake1", "/tmp/gtfs_fake2"]
        ):
            # Should not raise exception
            cleanup_old_gtfs_temp_dirs()


@pytest.mark.unit
class TestGetLatestGTFSArchiveFromCache:
    """Test get_latest_gtfs_archive_from_cache function."""

    @patch("amtraker_ingestion.chalicelib.utils.s3_client")
    def test_downloads_and_extracts_gtfs(self, mock_s3, temp_dir: str):
        """Test successful download and extraction from S3."""
        # Create a sample GTFS zip file
        gtfs_zip_path = Path(temp_dir) / "gtfs.zip"
        gtfs_content_dir = Path(temp_dir) / "content"
        gtfs_content_dir.mkdir()

        # Create a sample file to zip
        (gtfs_content_dir / "stops.txt").write_text("stop_id,stop_name\n")

        with ZipFile(gtfs_zip_path, "w") as zip_file:
            zip_file.write(gtfs_content_dir / "stops.txt", "stops.txt")

        # Mock s3_client.download_file to copy our sample zip
        def mock_download(bucket, key, filename):
            shutil.copy(gtfs_zip_path, filename)

        mock_s3.download_file = Mock(side_effect=mock_download)

        result = get_latest_gtfs_archive_from_cache("Amtrak")

        assert result is not None
        assert os.path.exists(result)
        assert os.path.exists(os.path.join(result, "stops.txt"))

        # Cleanup
        shutil.rmtree(os.path.dirname(result))

    @patch("amtraker_ingestion.chalicelib.utils.s3_client")
    def test_returns_none_when_file_not_found(self, mock_s3):
        """Test returns None when GTFS file doesn't exist in S3."""
        error_response = {"Error": {"Code": "NoSuchKey"}}
        mock_s3.download_file.side_effect = ClientError(
            error_response, "download_file"
        )

        result = get_latest_gtfs_archive_from_cache("NonExistent")
        assert result is None

    @patch("amtraker_ingestion.chalicelib.utils.s3_client")
    def test_raises_on_other_s3_errors(self, mock_s3):
        """Test raises exception for non-404 S3 errors."""
        error_response = {"Error": {"Code": "AccessDenied"}}
        mock_s3.download_file.side_effect = ClientError(
            error_response, "download_file"
        )

        with pytest.raises(ClientError):
            get_latest_gtfs_archive_from_cache("Amtrak")


@pytest.mark.unit
class TestGetLatestGTFSArchive:
    """Test get_latest_gtfs_archive function."""

    def test_downloads_and_extracts_gtfs_from_url(self, temp_dir: str):
        """Test successful download and extraction from URL."""
        # Create a sample GTFS zip file to serve
        gtfs_zip_path = Path(temp_dir) / "gtfs.zip"
        gtfs_content_dir = Path(temp_dir) / "content"
        gtfs_content_dir.mkdir()

        (gtfs_content_dir / "routes.txt").write_text("route_id,route_name\n")

        with ZipFile(gtfs_zip_path, "w") as zip_file:
            zip_file.write(gtfs_content_dir / "routes.txt", "routes.txt")

        # Mock urlretrieve to use our local file
        def mock_urlretrieve(url, filename):
            shutil.copy(gtfs_zip_path, filename)
            return filename, None

        with patch(
            "amtraker_ingestion.chalicelib.utils.urlretrieve",
            side_effect=mock_urlretrieve,
        ):
            result = get_latest_gtfs_archive("http://example.com/gtfs.zip")

        assert result is not None
        assert os.path.exists(result)
        assert os.path.exists(os.path.join(result, "routes.txt"))

        # Cleanup
        shutil.rmtree(os.path.dirname(result))

    def test_raises_on_download_error(self):
        """Test raises exception when download fails."""
        with patch(
            "amtraker_ingestion.chalicelib.utils.urlretrieve",
            side_effect=Exception("Network error"),
        ):
            with pytest.raises(Exception):
                get_latest_gtfs_archive("http://example.com/invalid.zip")


@pytest.mark.unit
class TestTrainsToList:
    """Test trains_to_list function."""

    def test_converts_train_response_to_list(self):
        """Test conversion of TrainResponse to list of dictionaries."""
        # This test would need actual TrainResponse model data
        # Mock the structure for now
        mock_response = Mock()
        mock_train = Mock()
        mock_train.model_dump.return_value = {
            "trainID": "2150-15",
            "trainNum": "2150",
        }
        mock_response.root = {"2150-15": [mock_train]}

        result = trains_to_list(mock_response)

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["trainID"] == "2150-15"

    def test_handles_multiple_trains(self):
        """Test conversion with multiple trains."""
        mock_response = Mock()

        mock_train1 = Mock()
        mock_train1.model_dump.return_value = {"trainID": "2150-15"}

        mock_train2 = Mock()
        mock_train2.model_dump.return_value = {"trainID": "2151-15"}

        mock_response.root = {
            "2150-15": [mock_train1],
            "2151-15": [mock_train2],
        }

        result = trains_to_list(mock_response)

        assert len(result) == 2
        assert result[0]["trainID"] == "2150-15"
        assert result[1]["trainID"] == "2151-15"
