from pathlib import Path
from read import read_amtraker_data
from transform import add_direction_id, add_scheduled_metrics
from gtfs import (
    generate_direction_on_custom_headsign,
    get_gtfs_last_modified,
    upload_gtfs_bundle,
)
from timefilter import set_last_processed
from write import add_service_dates, write_amtraker_events
from utils import get_latest_gtfs_archive, get_latest_gtfs_archive_from_cache
from constants import (
    AMTRAK_STATIC_GTFS,
    VIA_RAIL_STATIC_GTFS,
    BRIGHTLINE_STATIC_GTFS,
    S3_BUCKET,
)
from config import AMTRAK_ENABLED, VIA_ENABLED, BRIGHTLINE_ENABLED, ENVIRONMENT
from s3_upload import get_s3_json, set_s3_json
import urllib.request


def generate_event_data():
    amtrak = AMTRAK_ENABLED
    via = VIA_ENABLED
    brightline = BRIGHTLINE_ENABLED

    # Read and enrich Amtraker data
    amtrak_df, via_df, brightline_df = read_amtraker_data()
    if amtrak:
        # Amtrak
        amtrak_gtfs_dir = get_latest_gtfs_archive_from_cache("Amtrak")
        if amtrak_gtfs_dir is None:
            amtrak_gtfs_dir = get_latest_gtfs_archive(AMTRAK_STATIC_GTFS)
        # Enrich with direction_id and scheduled metrics
        enriched_amtrak = add_direction_id(amtrak_df, amtrak_gtfs_dir)
        enriched_amtrak = add_scheduled_metrics(enriched_amtrak, amtrak_gtfs_dir)
        # Add service dates
        enriched_amtrak = add_service_dates(enriched_amtrak)
        # Write events to CSV

        write_amtraker_events(enriched_amtrak)

    if via:
        via_gtfs_dir = get_latest_gtfs_archive_from_cache("Via")
        if via_gtfs_dir is None:
            via_gtfs_dir = get_latest_gtfs_archive(VIA_RAIL_STATIC_GTFS)
        # Via Rail
        enriched_via = add_direction_id(via_df, via_gtfs_dir)
        enriched_via = add_scheduled_metrics(enriched_via, via_gtfs_dir)
        # Add service dates
        enriched_via = add_service_dates(enriched_via)

        write_amtraker_events(enriched_via, "VIA")

    if brightline:
        brightline_gtfs_dir = get_latest_gtfs_archive_from_cache("Brightline")
        if brightline_gtfs_dir is None:
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


def check_gtfs_bundle_loop():
    # First download JSON, initialize empty cache if doesn't exist
    try:
        cache_data = get_s3_json(S3_BUCKET, "GTFS/last_modified.json")
    except Exception:
        # Cache file doesn't exist yet - initialize empty cache
        cache_data = {}
    cache_updated = False
    bundles = [
        (AMTRAK_STATIC_GTFS, "Amtrak"),
        (VIA_RAIL_STATIC_GTFS, "Via"),
        (BRIGHTLINE_STATIC_GTFS, "Brightline"),
    ]

    # Early exit check: see if all bundles are up-to-date before downloading
    all_up_to_date = True
    for bundle, agency in bundles:
        last_modified_date = get_gtfs_last_modified(bundle)
        if not last_modified_date:
            all_up_to_date = False
            break
        if agency not in cache_data or "last_modified" not in cache_data[agency]:
            all_up_to_date = False
            break
        # Convert datetime to ISO format string for comparison
        last_modified_str = last_modified_date.isoformat()
        if last_modified_str != cache_data[agency]["last_modified"]:
            all_up_to_date = False
            break

    if all_up_to_date:
        return  # All bundles are current, no work needed

    for bundle, agency in bundles:
        last_modified_date = get_gtfs_last_modified(bundle)
        if last_modified_date:
            # Convert datetime to ISO format string for JSON storage
            last_modified_str = last_modified_date.isoformat()

            if agency not in cache_data or "last_modified" not in cache_data[agency]:
                # No cache entry - download the bundle
                local_filename, headers = urllib.request.urlretrieve(bundle)
                upload_gtfs_bundle(
                    Path(local_filename), S3_BUCKET, f"GTFS/{agency}.zip"
                )
                if agency not in cache_data:
                    cache_data[agency] = {}
                cache_data[agency]["last_modified"] = last_modified_str
                cache_updated = True
            else:
                # Cache exists - check if it's outdated
                if last_modified_str != cache_data[agency]["last_modified"]:
                    # Download updated bundle
                    local_filename, headers = urllib.request.urlretrieve(bundle)
                    upload_gtfs_bundle(
                        Path(local_filename), S3_BUCKET, f"GTFS/{agency}.zip"
                    )
                    cache_data[agency]["last_modified"] = last_modified_str
                    cache_updated = True
    if cache_updated:
        set_s3_json(cache_data, S3_BUCKET, "GTFS/last_modified.json")


if __name__ == "__main__":
    generate_event_data()
    # check_gtfs_bundle_loop()
