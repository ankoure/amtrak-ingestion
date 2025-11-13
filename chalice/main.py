from read import read_amtraker_data
from transform import add_direction_id, add_scheduled_metrics
from gtfs import generate_direction_on_custom_headsign
from timefilter import set_last_processed
from write import add_service_dates, write_amtraker_events
from utils import get_latest_gtfs_archive
from constants import (
    AMTRAK_STATIC_GTFS,
    VIA_RAIL_STATIC_GTFS,
    BRIGHTLINE_STATIC_GTFS,
)
from config import AMTRAK_ENABLED, VIA_ENABLED, BRIGHTLINE_ENABLED, ENVIRONMENT


def generate_event_data():
    amtrak = AMTRAK_ENABLED
    via = VIA_ENABLED
    brightline = BRIGHTLINE_ENABLED

    # Read and enrich Amtraker data
    amtrak_df, via_df, brightline_df = read_amtraker_data()
    if amtrak:
        # Amtrak
        amtrak_gtfs_dir = get_latest_gtfs_archive(AMTRAK_STATIC_GTFS)
        # Enrich with direction_id and scheduled metrics
        enriched_amtrak = add_direction_id(amtrak_df, amtrak_gtfs_dir)
        enriched_amtrak = add_scheduled_metrics(enriched_amtrak, amtrak_gtfs_dir)
        # Add service dates
        enriched_amtrak = add_service_dates(enriched_amtrak)
        # Write events to CSV

        write_amtraker_events(enriched_amtrak)

    if via:
        via_gtfs_dir = get_latest_gtfs_archive(VIA_RAIL_STATIC_GTFS)
        # Via Rail
        enriched_via = add_direction_id(via_df, via_gtfs_dir)
        enriched_via = add_scheduled_metrics(enriched_via, via_gtfs_dir)
        # Add service dates
        enriched_via = add_service_dates(enriched_via)

        write_amtraker_events(enriched_via, "VIA")

    if brightline:
        brightline_gtfs_dir = get_latest_gtfs_archive(BRIGHTLINE_STATIC_GTFS)

        # Brightline
        brightline_lookup = {"EKW": 1, "MCO": 0, "WPT": 1}
        enriched_brightline = generate_direction_on_custom_headsign(
            brightline_df, brightline_lookup
        )
        enriched_brightline = add_scheduled_metrics(
            enriched_brightline, brightline_gtfs_dir
        )
        # Add service dates
        enriched_brightline = add_service_dates(enriched_brightline)

        write_amtraker_events(enriched_brightline, "Brightline")

        if ENVIRONMENT == "PROD":
            set_last_processed()


if __name__ == "__main__":
    generate_event_data()
