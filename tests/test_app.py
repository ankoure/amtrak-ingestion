import pytest
from unittest.mock import patch
from chalice.test import Client

from amtraker_ingestion.app import app


@pytest.fixture
def chalice_client():
    """Create a Chalice test client."""
    with Client(app) as client:
        yield client


@pytest.mark.integration
class TestHealthCheckEndpoint:
    """Test the health check endpoint."""

    def test_index_returns_hello_world(self, chalice_client):
        """Test that the index endpoint returns expected response."""
        response = chalice_client.http.get("/")

        assert response.status_code == 200
        assert response.json_body == {"hello": "world"}


@pytest.mark.integration
class TestManualGTFSUpdate:
    """Test manual GTFS update endpoint."""

    @patch("amtraker_ingestion.app.check_gtfs_bundle_loop")
    def test_manual_gtfs_update_success(self, mock_check_gtfs, chalice_client):
        """Test successful manual GTFS update."""
        mock_check_gtfs.return_value = None

        response = chalice_client.http.post("/gtfs/update")

        assert response.status_code == 200
        assert response.json_body["status"] == "completed"
        assert "duration_seconds" in response.json_body
        mock_check_gtfs.assert_called_once()

    @patch("amtraker_ingestion.app.check_gtfs_bundle_loop")
    def test_manual_gtfs_update_failure(self, mock_check_gtfs, chalice_client):
        """Test manual GTFS update with error."""
        mock_check_gtfs.side_effect = Exception("GTFS download failed")

        response = chalice_client.http.post("/gtfs/update")
        # Chalice returns 500 for unhandled exceptions
        assert response.status_code == 500


@pytest.mark.integration
class TestManualAmtrakerUpdate:
    """Test manual Amtraker update endpoint."""

    @patch("amtraker_ingestion.app.generate_event_data")
    def test_manual_amtraker_update_success(
        self, mock_generate, chalice_client
    ):
        """Test successful manual Amtraker update."""
        mock_generate.return_value = None

        response = chalice_client.http.post("/amtraker/update")

        assert response.status_code == 200
        assert response.json_body["status"] == "completed"
        assert "duration_seconds" in response.json_body
        mock_generate.assert_called_once()

    @patch("amtraker_ingestion.app.generate_event_data")
    def test_manual_amtraker_update_failure(
        self, mock_generate, chalice_client
    ):
        """Test manual Amtraker update with error."""
        mock_generate.side_effect = Exception("API connection failed")

        response = chalice_client.http.post("/amtraker/update")
        # Chalice returns 500 for unhandled exceptions
        assert response.status_code == 500


@pytest.mark.integration
class TestManualCollateData:
    """Test manual data collation endpoint."""

    @patch("amtraker_ingestion.app.collate_previous_day_data")
    def test_collate_previous_day_no_params(
        self, mock_collate, chalice_client
    ):
        """Test collation without parameters (previous day)."""
        mock_collate.return_value = None

        response = chalice_client.http.post(
            "/amtraker/collate",
            body=b"{}",
            headers={"Content-Type": "application/json"},
        )

        assert response.status_code == 200
        assert response.json_body["status"] == "completed"
        assert "previous day" in response.json_body["message"]
        mock_collate.assert_called_once()

    @patch("amtraker_ingestion.app.collate_amtraker_data_for_date")
    def test_collate_specific_date(self, mock_collate_date, chalice_client):
        """Test collation for a specific date."""
        mock_collate_date.return_value = [{"event": "data"}] * 100

        response = chalice_client.http.post(
            "/amtraker/collate",
            body=b'{"year": 2025, "month": 1, "day": 15}',
            headers={"Content-Type": "application/json"},
        )

        assert response.status_code == 200
        assert response.json_body["status"] == "completed"
        assert response.json_body["events_count"] == 100
        assert response.json_body["year"] == 2025
        assert response.json_body["month"] == 1
        assert response.json_body["day"] == 15
        mock_collate_date.assert_called_once_with(2025, 1, 15, "Amtrak")

    @patch("amtraker_ingestion.app.collate_amtraker_data_for_date")
    def test_collate_with_custom_mode(self, mock_collate_date, chalice_client):
        """Test collation with custom provider mode."""
        from amtraker_ingestion.chalicelib.constants import Provider

        mock_collate_date.return_value = []

        response = chalice_client.http.post(
            "/amtraker/collate",
            body=b'{"year": 2025, "month": 1, "day": 15, "mode": "VIA"}',
            headers={"Content-Type": "application/json"},
        )

        assert response.status_code == 200
        # Verify the Provider enum was passed correctly
        call_args = mock_collate_date.call_args[0]
        assert call_args[3] == Provider.VIA

    @patch("amtraker_ingestion.app.collate_previous_day_data")
    def test_collate_partial_date_params(self, mock_collate, chalice_client):
        """Test partial date parameters falls back to previous day."""
        mock_collate.return_value = None

        response = chalice_client.http.post(
            "/amtraker/collate",
            body=b'{"year": 2025}',
            headers={"Content-Type": "application/json"},
        )

        assert response.status_code == 200
        assert "previous day" in response.json_body["message"]
        mock_collate.assert_called_once()


@pytest.mark.integration
class TestScheduledFunctions:
    """Test scheduled Lambda functions.

    Note: Testing scheduled functions directly is complex with Chalice
    event handlers. These tests are skipped in favor of testing via
    the manual HTTP endpoints which call the same underlying functions.
    """

    @pytest.mark.skip(reason="Chalice scheduled functions need full event")
    @patch("amtraker_ingestion.app.check_gtfs_bundle_loop")
    def test_update_gtfs_cache_scheduled(self, mock_check_gtfs):
        """Test scheduled GTFS cache update."""
        pass

    @pytest.mark.skip(reason="Chalice scheduled functions need full event")
    @patch("amtraker_ingestion.app.check_gtfs_bundle_loop")
    def test_update_gtfs_cache_logs_errors(self, mock_check_gtfs):
        """Test that scheduled GTFS update raises errors."""
        pass

    @pytest.mark.skip(reason="Chalice scheduled functions need full event")
    @patch("amtraker_ingestion.app.generate_event_data")
    def test_consume_amtraker_api_scheduled(self, mock_generate):
        """Test scheduled Amtraker API consumption."""
        pass

    @pytest.mark.skip(reason="Chalice scheduled functions need full event")
    @patch("amtraker_ingestion.app.collate_previous_day_data")
    def test_collate_previous_day_scheduled(self, mock_collate):
        """Test scheduled data collation."""
        pass
