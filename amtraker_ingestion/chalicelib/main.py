from pathlib import Path
from chalicelib.read import read_amtraker_data
from chalicelib.transform import add_direction_id, add_scheduled_metrics
from chalicelib.gtfs import (
    generate_direction_on_custom_headsign,
    get_gtfs_last_modified,
    upload_gtfs_bundle,
)
from chalicelib.timefilter import set_last_processed
from chalicelib.write import add_service_dates, write_amtraker_events
from chalicelib.utils import (
    get_latest_gtfs_archive,
    get_latest_gtfs_archive_from_cache,
    cleanup_old_gtfs_temp_dirs,
)
from chalicelib.constants import (
    AMTRAK_STATIC_GTFS,
    VIA_RAIL_STATIC_GTFS,
    BRIGHTLINE_STATIC_GTFS,
    S3_BUCKET,
    EASTERN_TIME,
    LOCAL_DATA_TEMPLATE,
    Provider,
)
from chalicelib.config import (
    AMTRAK_ENABLED,
    VIA_ENABLED,
    BRIGHTLINE_ENABLED,
    ENVIRONMENT,
    get_logger,
)
from chalicelib.s3_upload import (
    get_s3_json,
    set_s3_json,
    s3_client,
    _compress_and_upload_file,
)
import urllib.request
from datetime import datetime, timedelta
from chalicelib.disk import write_event
import glob
import gzip
import json
import time

logger = get_logger(__name__)


def generate_event_data():
    """
    Main function to generate event data from Amtraker API.
    Reads, enriches, and writes events for all enabled providers.
    """
    start_time = time.time()
    logger.info("Starting event data generation")

    # Clean up any old GTFS temp directories to prevent disk space issues
    cleanup_old_gtfs_temp_dirs()

    amtrak = AMTRAK_ENABLED
    via = VIA_ENABLED
    brightline = BRIGHTLINE_ENABLED

    enabled_providers = []
    if amtrak:
        enabled_providers.append("Amtrak")
    if via:
        enabled_providers.append("Via")
    if brightline:
        enabled_providers.append("Brightline")

    logger.info(
        f"Enabled providers: {', '.join(enabled_providers) if enabled_providers else 'None'}"
    )

    try:
        # Read and enrich Amtraker data
        read_start = time.time()
        amtrak_df, via_df, brightline_df = read_amtraker_data()
        read_duration = time.time() - read_start

        logger.info(
            f"API data read completed in {read_duration:.2f}s - "
            f"Amtrak: {len(amtrak_df)} rows, "
            f"Via: {len(via_df)} rows, "
            f"Brightline: {len(brightline_df)} rows"
        )

        if amtrak:
            provider_start = time.time()
            logger.info("Processing Amtrak data")

            # Get GTFS archive
            amtrak_gtfs_dir = get_latest_gtfs_archive_from_cache("Amtrak")
            if amtrak_gtfs_dir is None:
                logger.warning(
                    "Amtrak GTFS not in cache, downloading from source"
                )
                amtrak_gtfs_dir = get_latest_gtfs_archive(AMTRAK_STATIC_GTFS)
            else:
                logger.debug(f"Using cached Amtrak GTFS: {amtrak_gtfs_dir}")

            # Enrich with direction_id and scheduled metrics
            enriched_amtrak = add_direction_id(amtrak_df, amtrak_gtfs_dir)
            enriched_amtrak = add_scheduled_metrics(
                enriched_amtrak, amtrak_gtfs_dir
            )
            # Add service dates
            enriched_amtrak = add_service_dates(enriched_amtrak)

            # Write events
            write_amtraker_events(enriched_amtrak)

            provider_duration = time.time() - provider_start
            logger.info(
                f"Amtrak processing completed in {provider_duration:.2f}s - "
                f"{len(enriched_amtrak)} events"
            )

        if via:
            provider_start = time.time()
            logger.info("Processing Via Rail data")

            via_gtfs_dir = get_latest_gtfs_archive_from_cache("Via")
            if via_gtfs_dir is None:
                logger.warning(
                    "Via Rail GTFS not in cache, downloading from source"
                )
                via_gtfs_dir = get_latest_gtfs_archive(VIA_RAIL_STATIC_GTFS)
            else:
                logger.debug(f"Using cached Via Rail GTFS: {via_gtfs_dir}")

            # Via Rail
            enriched_via = add_direction_id(via_df, via_gtfs_dir)
            enriched_via = add_scheduled_metrics(enriched_via, via_gtfs_dir)
            # Add service dates
            enriched_via = add_service_dates(enriched_via)

            write_amtraker_events(enriched_via, Provider.VIA)

            provider_duration = time.time() - provider_start
            logger.info(
                f"Via Rail processing completed in {provider_duration:.2f}s - "
                f"{len(enriched_via)} events"
            )

        if brightline:
            provider_start = time.time()
            logger.info("Processing Brightline data")

            brightline_gtfs_dir = get_latest_gtfs_archive_from_cache(
                "Brightline"
            )
            if brightline_gtfs_dir is None:
                logger.warning(
                    "Brightline GTFS not in cache, downloading from source"
                )
                brightline_gtfs_dir = get_latest_gtfs_archive(
                    BRIGHTLINE_STATIC_GTFS
                )
            else:
                logger.debug(
                    f"Using cached Brightline GTFS: {brightline_gtfs_dir}"
                )

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

            provider_duration = time.time() - provider_start
            logger.info(
                f"Brightline processing completed in {provider_duration:.2f}s - "
                f"{len(enriched_brightline)} events"
            )

        if ENVIRONMENT == "PROD":
            set_last_processed()
            logger.debug("Last processed timestamp updated")

        # Clean up GTFS temp directories after processing
        cleanup_old_gtfs_temp_dirs()

        total_duration = time.time() - start_time
        logger.info(
            f"Event data generation completed successfully in {total_duration:.2f}s"
        )

    except Exception as e:
        logger.error(
            f"Error during event data generation: {e}",
            exc_info=True
        )
        raise


def check_gtfs_bundle_loop():
    """
    Check and update GTFS bundles for all providers.
    Downloads new bundles if they've been updated since last check.
    """
    start_time = time.time()
    logger.info("Starting GTFS bundle check")

    # First download JSON, initialize empty cache if doesn't exist
    try:
        cache_data = get_s3_json(S3_BUCKET, "GTFS/last_modified.json")
        logger.debug("Loaded GTFS cache metadata from S3")
    except Exception as e:
        # Cache file doesn't exist yet - initialize empty cache
        logger.info(
            f"GTFS cache metadata not found, initializing: {e}"
        )
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
            logger.warning(
                f"Could not get last modified date for {agency} GTFS"
            )
            all_up_to_date = False
            break
        if agency not in cache_data or "last_modified" not in cache_data[agency]:
            logger.debug(f"No cache entry found for {agency}")
            all_up_to_date = False
            break
        # Convert datetime to ISO format string for comparison
        last_modified_str = last_modified_date.isoformat()
        if last_modified_str != cache_data[agency]["last_modified"]:
            logger.info(
                f"{agency} GTFS bundle has been updated - "
                f"Cached: {cache_data[agency]['last_modified']}, "
                f"Current: {last_modified_str}"
            )
            all_up_to_date = False
            break

    if all_up_to_date:
        logger.info("All GTFS bundles are up-to-date, no updates needed")
        return  # All bundles are current, no work needed

    downloads_count = 0
    for bundle, agency in bundles:
        last_modified_date = get_gtfs_last_modified(bundle)
        if last_modified_date:
            # Convert datetime to ISO format string for JSON storage
            last_modified_str = last_modified_date.isoformat()

            if agency not in cache_data or "last_modified" not in cache_data[agency]:
                # No cache entry - download the bundle
                logger.info(f"Downloading {agency} GTFS bundle (new)")
                download_start = time.time()

                local_filename, headers = urllib.request.urlretrieve(bundle)
                upload_gtfs_bundle(
                    Path(local_filename), S3_BUCKET, f"GTFS/{agency}.zip"
                )

                download_duration = time.time() - download_start
                logger.info(
                    f"{agency} GTFS bundle downloaded and uploaded to S3 "
                    f"in {download_duration:.2f}s"
                )

                if agency not in cache_data:
                    cache_data[agency] = {}
                cache_data[agency]["last_modified"] = last_modified_str
                cache_updated = True
                downloads_count += 1
            else:
                # Cache exists - check if it's outdated
                if last_modified_str != cache_data[agency]["last_modified"]:
                    # Download updated bundle
                    logger.info(f"Downloading {agency} GTFS bundle (update)")
                    download_start = time.time()

                    local_filename, headers = urllib.request.urlretrieve(bundle)
                    upload_gtfs_bundle(
                        Path(local_filename), S3_BUCKET, f"GTFS/{agency}.zip"
                    )

                    download_duration = time.time() - download_start
                    logger.info(
                        f"{agency} GTFS bundle updated and uploaded to S3 "
                        f"in {download_duration:.2f}s"
                    )

                    cache_data[agency]["last_modified"] = last_modified_str
                    cache_updated = True
                    downloads_count += 1
                else:
                    logger.debug(f"{agency} GTFS bundle is up-to-date")

    if cache_updated:
        set_s3_json(cache_data, S3_BUCKET, "GTFS/last_modified.json")
        logger.info("GTFS cache metadata updated in S3")

    total_duration = time.time() - start_time
    logger.info(
        f"GTFS bundle check completed in {total_duration:.2f}s - "
        f"{downloads_count} bundle(s) downloaded"
    )


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
    date_str = f"{year}-{month:02d}-{day:02d}"
    logger.info(f"Collating data for {mode} on {date_str}")

    # Construct the S3 prefix for the specified day
    # Convert mode to string if it's a Provider enum
    mode_str = str(mode)
    prefix = (
        f"Events-live/data/raw/{mode_str}/"
        f"Year={year}/Month={month:02d}/Day={day:02d}/"
    )
    logger.debug(f"S3 prefix: {prefix}")

    all_events = []

    try:
        # List all objects with the given prefix
        list_start = time.time()
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        list_duration = time.time() - list_start

        if "Contents" not in response:
            logger.warning(
                f"No files found for {mode} on {date_str} "
                f"(checked in {list_duration:.2f}s)"
            )
            return all_events

        file_count = len(response["Contents"])
        json_file_count = sum(
            1 for obj in response["Contents"]
            if obj["Key"].endswith(".json.gz")
        )
        logger.info(
            f"Found {json_file_count} JSON files ({file_count} total objects) "
            f"in {list_duration:.2f}s"
        )

        # Iterate through all gzipped JSON files for the day
        process_start = time.time()
        for obj in response["Contents"]:
            key = obj["Key"]

            # Skip if not a JSON file
            if not key.endswith(".json.gz"):
                logger.debug(f"Skipping non-JSON file: {key}")
                continue

            # Download and decompress the file
            s3_object = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
            compressed_data = s3_object["Body"].read()
            compressed_size = len(compressed_data)

            # Decompress the gzipped data
            decompressed_data = gzip.decompress(compressed_data)
            decompressed_size = len(decompressed_data)

            # Parse JSON
            events = json.loads(decompressed_data.decode("utf-8"))

            # Add all events from this file to our collection
            if isinstance(events, list):
                all_events.extend(events)
            else:
                all_events.append(events)

            logger.debug(
                f"Processed {key}: "
                f"{compressed_size} bytes compressed, "
                f"{decompressed_size} bytes decompressed, "
                f"{len(events) if isinstance(events, list) else 1} events"
            )

        process_duration = time.time() - process_start
        logger.info(
            f"Collected {len(all_events)} events from {json_file_count} files "
            f"for {mode} on {date_str} in {process_duration:.2f}s"
        )

    except Exception as e:
        logger.error(
            f"Error collating data for {mode} on {date_str}: {e}",
            exc_info=True
        )
        raise

    return all_events


def collate_amtraker_data():
    """
    Collate the previous day's data for all providers.
    Writes out the collated data as gzipped CSVs and uploads to S3.
    This is meant to be called by the scheduled cron job.
    """
    start_time = time.time()
    logger.info("Starting daily data collation")

    # Get yesterday's date
    yesterday = datetime.now(EASTERN_TIME) - timedelta(days=1)
    year = yesterday.year
    month = yesterday.month
    day = yesterday.day

    logger.info(f"Collating data for date: {year}-{month:02d}-{day:02d}")

    # Collate data for each provider
    providers = []
    if AMTRAK_ENABLED:
        providers.append(Provider.AMTRAK)
    if VIA_ENABLED:
        providers.append(Provider.VIA)
    if BRIGHTLINE_ENABLED:
        providers.append(Provider.BRIGHTLINE)

    logger.info(f"Providers to collate: {[str(p) for p in providers]}")

    total_events = 0
    total_uploads = 0

    for provider in providers:
        provider_start = time.time()
        try:
            # Get all events for the day
            events = collate_amtraker_data_for_date(year, month, day, provider)

            # Write each event to CSV files (organized by route/direction/stop)
            write_start = time.time()
            for event in events:
                write_event(event, provider)
            write_duration = time.time() - write_start

            logger.info(
                f"Wrote {len(events)} events for {provider} to CSV files "
                f"in {write_duration:.2f}s"
            )

            # Find all CSV files for this day and upload them to S3
            files_for_day = glob.glob(
                LOCAL_DATA_TEMPLATE.format(year=year, month=month, day=day)
            )

            uploaded_count = 0
            upload_start = time.time()
            for fp in files_for_day:
                # Check if file belongs to this provider by checking the path
                provider_str = str(provider)
                if f"daily-{provider_str}-data" in fp:
                    _compress_and_upload_file(fp)
                    uploaded_count += 1
            upload_duration = time.time() - upload_start

            provider_duration = time.time() - provider_start
            logger.info(
                f"{provider} collation completed in {provider_duration:.2f}s - "
                f"{len(events)} events, {uploaded_count} files uploaded "
                f"(upload took {upload_duration:.2f}s)"
            )

            total_events += len(events)
            total_uploads += uploaded_count

        except Exception as e:
            logger.error(
                f"Error collating {provider} data: {e}",
                exc_info=True
            )

    total_duration = time.time() - start_time
    logger.info(
        f"Daily data collation completed in {total_duration:.2f}s - "
        f"{total_events} total events, {total_uploads} files uploaded"
    )


if __name__ == "__main__":
    generate_event_data()
    check_gtfs_bundle_loop()
