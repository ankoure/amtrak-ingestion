import pytest
import polars as pl
from unittest.mock import patch

from amtraker_ingestion.chalicelib.transform import (
    add_direction_id,
    add_scheduled_metrics,
)


@pytest.mark.unit
class TestAddDirectionId:
    """Test add_direction_id function."""

    def test_adds_direction_id_with_primary_lookup(self, mock_gtfs_dir: str):
        """Test adding direction_id using primary lookup (trainNumRaw)."""
        # Create sample Amtraker data
        amtraker_data = pl.DataFrame(
            {
                "trainNumRaw": ["2150", "2151"],
                "destCode": ["NYP", "BOS"],
            }
        )

        with patch(
            "amtraker_ingestion.chalicelib.transform.generate_direction_lookup"
        ) as mock_lookup:
            # Mock the primary and secondary lookups
            primary_lookup = pl.DataFrame(
                {
                    "trip_short_name": ["2150", "2151"],
                    "direction_id": [0, 1],
                }
            )
            secondary_lookup = pl.DataFrame(
                {
                    "headsign_stop_id": ["NYP", "BOS"],
                    "direction_id": [0, 1],
                }
            )
            mock_lookup.return_value = (primary_lookup, secondary_lookup)

            result = add_direction_id(amtraker_data, mock_gtfs_dir)

        assert "direction_id" in result.columns
        assert "lookup_method" in result.columns
        assert result["direction_id"].to_list() == [0, 1]
        assert all(
            method == "primary" for method in result["lookup_method"].to_list()
        )

    def test_adds_direction_id_with_secondary_lookup(self, mock_gtfs_dir: str):
        """Test adding direction_id using secondary lookup (destCode)."""
        amtraker_data = pl.DataFrame(
            {
                "trainNumRaw": ["9999"],  # Not in primary lookup
                "destCode": ["NYP"],
            }
        )

        with patch(
            "amtraker_ingestion.chalicelib.transform.generate_direction_lookup"
        ) as mock_lookup:
            primary_lookup = pl.DataFrame(
                {
                    "trip_short_name": ["2150"],
                    "direction_id": [0],
                }
            )
            secondary_lookup = pl.DataFrame(
                {
                    "headsign_stop_id": ["NYP"],
                    "direction_id": [0],
                }
            )
            mock_lookup.return_value = (primary_lookup, secondary_lookup)

            result = add_direction_id(amtraker_data, mock_gtfs_dir)

        assert result["direction_id"][0] == 0
        assert result["lookup_method"][0] == "secondary"

    def test_handles_missing_lookups(self, mock_gtfs_dir: str):
        """Test handling when neither lookup matches."""
        amtraker_data = pl.DataFrame(
            {
                "trainNumRaw": ["9999"],
                "destCode": ["UNKNOWN"],
            }
        )

        with patch(
            "amtraker_ingestion.chalicelib.transform.generate_direction_lookup"
        ) as mock_lookup:
            primary_lookup = pl.DataFrame(
                {
                    "trip_short_name": ["2150"],
                    "direction_id": [0],
                }
            )
            secondary_lookup = pl.DataFrame(
                {
                    "headsign_stop_id": ["NYP"],
                    "direction_id": [0],
                }
            )
            mock_lookup.return_value = (primary_lookup, secondary_lookup)

            result = add_direction_id(amtraker_data, mock_gtfs_dir)

        assert result["direction_id"][0] is None
        assert result["lookup_method"][0] is None

    def test_prefers_primary_over_secondary(self, mock_gtfs_dir: str):
        """Test that primary lookup is preferred when both match."""
        amtraker_data = pl.DataFrame(
            {
                "trainNumRaw": ["2150"],
                "destCode": ["NYP"],
            }
        )

        with patch(
            "amtraker_ingestion.chalicelib.transform.generate_direction_lookup"
        ) as mock_lookup:
            # Both lookups match but with different direction_ids
            primary_lookup = pl.DataFrame(
                {
                    "trip_short_name": ["2150"],
                    "direction_id": [0],
                }
            )
            secondary_lookup = pl.DataFrame(
                {
                    "headsign_stop_id": ["NYP"],
                    "direction_id": [1],
                }
            )
            mock_lookup.return_value = (primary_lookup, secondary_lookup)

            result = add_direction_id(amtraker_data, mock_gtfs_dir)

        # Should use primary lookup (direction_id = 0)
        assert result["direction_id"][0] == 0
        assert result["lookup_method"][0] == "primary"


@pytest.mark.unit
class TestAddScheduledMetrics:
    """Test add_scheduled_metrics function."""

    def test_adds_scheduled_metrics(self, mock_gtfs_dir: str):
        """Test adding scheduled headway and travel time metrics."""
        amtraker_data = pl.DataFrame(
            {
                "code": ["BOS", "NYP"],
                "direction_id": [0, 0],
            }
        )

        with patch(
            "amtraker_ingestion.chalicelib.transform.calculate_gtfs_metrics"
        ) as mock_metrics:
            # Mock GTFS metrics
            gtfs_metrics = pl.DataFrame(
                {
                    "stop_id": ["BOS", "BOS", "NYP", "NYP"],
                    "direction_id": [0, 0, 0, 0],
                    "scheduled_headway": [3600, 3600, 7200, 7200],
                    "scheduled_tt": [0, 0, 14400, 14400],
                }
            )
            mock_metrics.return_value = gtfs_metrics

            result = add_scheduled_metrics(amtraker_data, mock_gtfs_dir)

        assert "scheduled_headway" in result.columns
        assert "scheduled_tt" in result.columns
        assert result["scheduled_headway"][0] == 3600
        assert result["scheduled_tt"][1] == 14400

    def test_handles_missing_metrics(self, mock_gtfs_dir: str):
        """Test handling when metrics are not found for a stop."""
        amtraker_data = pl.DataFrame(
            {
                "code": ["UNKNOWN"],
                "direction_id": [0],
            }
        )

        with patch(
            "amtraker_ingestion.chalicelib.transform.calculate_gtfs_metrics"
        ) as mock_metrics:
            gtfs_metrics = pl.DataFrame(
                {
                    "stop_id": ["BOS"],
                    "direction_id": [0],
                    "scheduled_headway": [3600],
                    "scheduled_tt": [0],
                }
            )
            mock_metrics.return_value = gtfs_metrics

            result = add_scheduled_metrics(amtraker_data, mock_gtfs_dir)

        assert result["scheduled_headway"][0] is None
        assert result["scheduled_tt"][0] is None

    def test_aggregates_multiple_trips(self, mock_gtfs_dir: str):
        """Test that metrics are averaged when multiple trips exist."""
        amtraker_data = pl.DataFrame(
            {
                "code": ["BOS"],
                "direction_id": [0],
            }
        )

        with patch(
            "amtraker_ingestion.chalicelib.transform.calculate_gtfs_metrics"
        ) as mock_metrics:
            # Multiple trips for the same stop with different metrics
            gtfs_metrics = pl.DataFrame(
                {
                    "stop_id": ["BOS", "BOS", "BOS"],
                    "direction_id": [0, 0, 0],
                    "scheduled_headway": [3600, 7200, 5400],
                    "scheduled_tt": [0, 0, 0],
                }
            )
            mock_metrics.return_value = gtfs_metrics

            result = add_scheduled_metrics(amtraker_data, mock_gtfs_dir)

        # Should be averaged: (3600 + 7200 + 5400) / 3 = 5400
        assert result["scheduled_headway"][0] == 5400

    def test_converts_metrics_to_integers(self, mock_gtfs_dir: str):
        """Test that metrics are converted to integers after aggregation."""
        amtraker_data = pl.DataFrame(
            {
                "code": ["BOS"],
                "direction_id": [0],
            }
        )

        with patch(
            "amtraker_ingestion.chalicelib.transform.calculate_gtfs_metrics"
        ) as mock_metrics:
            gtfs_metrics = pl.DataFrame(
                {
                    "stop_id": ["BOS", "BOS"],
                    "direction_id": [0, 0],
                    "scheduled_headway": [3600.5, 3601.5],
                    "scheduled_tt": [100.3, 100.7],
                }
            )
            mock_metrics.return_value = gtfs_metrics

            result = add_scheduled_metrics(amtraker_data, mock_gtfs_dir)

        # Should be rounded: (3600.5 + 3601.5) / 2 = 3601
        assert isinstance(result["scheduled_headway"][0], int)
        assert result["scheduled_headway"][0] == 3601
