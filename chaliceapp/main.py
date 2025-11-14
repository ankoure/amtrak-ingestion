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
    EASTERN_TIME,
    LOCAL_DATA_TEMPLATE,
    Provider,
)
from config import AMTRAK_ENABLED, VIA_ENABLED, BRIGHTLINE_ENABLED, ENVIRONMENT
from s3_upload import get_s3_json, set_s3_json, s3_client, _compress_and_upload_file
import urllib.request
from datetime import datetime, timedelta
from disk import write_event
import glob
import gzip
import json


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

        write_amtraker_events(enriched_via, Provider.VIA)

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

        write_amtraker_events(enriched_brightline, Provider.BRIGHTLINE)

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


def collate_amtraker_data_for_date(
    year: int, month: int, day: int, mode: Provider | str = Provider.AMTRAK
) -> list[dict]:
    """
    Collect all gzipped JSON data for a specified day from S3 into a list of dicts.

    Args:
        year: Year (e.g., 2025)
        month: Month (1-12)
        day: Day of month (1-31)
        mode: Provider name (e.g., "amtrak", "via", "brightline")

    Returns:
        List of event dictionaries from all JSON files for that day
    """

    # Construct the S3 prefix for the specified day
    # Convert mode to string if it's a Provider enum
    mode_str = str(mode)
    prefix = (
        f"Events-live/data/raw/{mode_str}/Year={year}/Month={month:02d}/Day={day:02d}/"
    )

    all_events = []

    try:
        # List all objects with the given prefix
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)

        if "Contents" not in response:
            print(f"No files found for {year}-{month:02d}-{day:02d}")
            return all_events

        # Iterate through all gzipped JSON files for the day
        for obj in response["Contents"]:
            key = obj["Key"]

            # Skip if not a JSON file
            if not key.endswith(".json.gz"):
                continue

            # Download and decompress the file
            s3_object = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
            compressed_data = s3_object["Body"].read()

            # Decompress the gzipped data
            decompressed_data = gzip.decompress(compressed_data)

            # Parse JSON
            events = json.loads(decompressed_data.decode("utf-8"))

            # Add all events from this file to our collection
            if isinstance(events, list):
                all_events.extend(events)
            else:
                all_events.append(events)

        print(
            f"Collected {len(all_events)} events from {len(response['Contents'])} files"
        )

    except Exception as e:
        print(f"Error collating data for {year}-{month:02d}-{day:02d}: {e}")
        raise

    return all_events


def collate_amtraker_data():
    """
    Collate the previous day's data for all providers.
    Writes out the collated data as gzipped CSVs and uploads to S3.
    This is meant to be called by the scheduled cron job.
    """

    # Get yesterday's date
    yesterday = datetime.now(EASTERN_TIME) - timedelta(days=1)
    year = yesterday.year
    month = yesterday.month
    day = yesterday.day

    # Collate data for each provider
    providers = []
    if AMTRAK_ENABLED:
        providers.append(Provider.AMTRAK)
    if VIA_ENABLED:
        providers.append(Provider.VIA)
    if BRIGHTLINE_ENABLED:
        providers.append(Provider.BRIGHTLINE)

    for provider in providers:
        try:
            # Get all events for the day
            events = collate_amtraker_data_for_date(year, month, day, provider)
            print(
                f"Collated {len(events)} events for {provider} on {year}-{month:02d}-{day:02d}"
            )

            # Write each event to CSV files (organized by route/direction/stop)
            for event in events:
                write_event(event, provider)

            # Find all CSV files for this day and upload them to S3
            files_for_day = glob.glob(
                LOCAL_DATA_TEMPLATE.format(year=year, month=month, day=day)
            )

            uploaded_count = 0
            for fp in files_for_day:
                # Check if file belongs to this provider by checking the path
                provider_str = str(provider)
                if f"daily-{provider_str}-data" in fp:
                    _compress_and_upload_file(fp)
                    uploaded_count += 1

            print(
                f"Uploaded {uploaded_count} gzipped CSV files for {provider} on {year}-{month:02d}-{day:02d}"
            )

        except Exception as e:
            print(f"Error collating {provider} data: {e}")


if __name__ == "__main__":
    generate_event_data()
    check_gtfs_bundle_loop()
