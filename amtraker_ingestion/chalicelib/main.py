"""
Main Pipeline Orchestration Module
===================================

This module contains the core pipeline functions for the Amtrak Ingestion system.
It orchestrates the entire data flow from API ingestion through enrichment to storage.

Main Functions
--------------
generate_event_data
    Main entry point for real-time event generation
check_gtfs_bundle_loop
    Checks and updates GTFS bundles for all providers
collate_amtraker_data
    Collates daily data from S3 into organized CSV files
"""

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
    LOCAL_DATA_TEMPLATE,
    Provider,
)
from chalicelib.config import (
    AMTRAK_ENABLED,
    VIA_ENABLED,
    BRIGHTLINE_ENABLED,
    ENVIRONMENT,
    get_logger,
    lambda_metric,
    get_dd_tags,
)
from chalicelib.s3_upload import (
    get_s3_json,
    set_s3_json,
    s3_client,
    _compress_and_upload_file,
)
import urllib.request
from datetime import datetime, timedelta, timezone
from chalicelib.disk import write_event
import glob
import gzip
import json
import time

logger = get_logger(__name__)


def generate_event_data():
    """
    Generate event data from Amtraker API for all enabled providers.

    This is the main entry point for the real-time data pipeline. It performs
    the following steps for each enabled provider:

    1. Fetches current train data from the Amtraker API
    2. Retrieves GTFS data from cache or downloads if needed
    3. Enriches data with direction IDs from GTFS
    4. Adds scheduled metrics (headway and travel time)
    5. Calculates service dates for events
    6. Writes arrival and departure events to files
    7. Uploads compressed data to S3

    Raises
    ------
    Exception
        If any critical error occurs during processing.

    Notes
    -----
    Provider enablement is controlled by flags in the config module:
    ``AMTRAK_ENABLED``, ``VIA_ENABLED``, ``BRIGHTLINE_ENABLED``.

    Examples
    --------
    >>> generate_event_data()  # Processes all enabled providers
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
            enriched_amtrak = add_direction_id(
                amtrak_df, amtrak_gtfs_dir, provider="amtrak"
            )
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
            lambda_metric(
                "pipeline.run.provider_duration_seconds",
                provider_duration,
                tags=get_dd_tags(provider="amtrak"),
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
            enriched_via = add_direction_id(
                via_df, via_gtfs_dir, provider="via"
            )
            enriched_via = add_scheduled_metrics(enriched_via, via_gtfs_dir)
            # Add service dates
            enriched_via = add_service_dates(enriched_via)

            write_amtraker_events(enriched_via, Provider.VIA)

            provider_duration = time.time() - provider_start
            logger.info(
                f"Via Rail processing completed in {provider_duration:.2f}s - "
                f"{len(enriched_via)} events"
            )
            lambda_metric(
                "pipeline.run.provider_duration_seconds",
                provider_duration,
                tags=get_dd_tags(provider="via"),
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
            lambda_metric(
                "pipeline.run.provider_duration_seconds",
                provider_duration,
                tags=get_dd_tags(provider="brightline"),
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
        lambda_metric(
            "pipeline.run.total_duration_seconds",
            total_duration,
            tags=get_dd_tags(),
        )

    except Exception as e:
        logger.error(f"Error during event data generation: {e}", exc_info=True)
        raise


def check_gtfs_bundle_loop():
    """
    Check and update GTFS bundles for all transit providers.

    This function checks the Last-Modified header of each provider's GTFS feed
    and downloads updated bundles if they have changed since the last check.
    Updated bundles are uploaded to S3 for caching.

    The function maintains a cache metadata file in S3 at
    ``GTFS/last_modified.json`` that tracks the last modified date for each
    provider's GTFS bundle.

    Providers Checked
    -----------------
    - Amtrak: National passenger railroad GTFS
    - VIA Rail: Canadian passenger railroad GTFS
    - Brightline: Florida high-speed rail GTFS

    Notes
    -----
    This function performs an early exit optimization: if all bundles are
    up-to-date based on the cache, no downloads are performed.

    See Also
    --------
    get_gtfs_last_modified : Gets Last-Modified header from GTFS URL
    upload_gtfs_bundle : Uploads GTFS zip file to S3
    """
    start_time = time.time()
    logger.info("Starting GTFS bundle check")

    # First download JSON, initialize empty cache if doesn't exist
    try:
        cache_data = get_s3_json(S3_BUCKET, "GTFS/last_modified.json")
        logger.debug("Loaded GTFS cache metadata from S3")
    except Exception as e:
        # Cache file doesn't exist yet - initialize empty cache
        logger.info(f"GTFS cache metadata not found, initializing: {e}")
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
        if (
            agency not in cache_data
            or "last_modified" not in cache_data[agency]
        ):
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

            if (
                agency not in cache_data
                or "last_modified" not in cache_data[agency]
            ):
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

                    local_filename, headers = urllib.request.urlretrieve(
                        bundle
                    )
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
    tags = get_dd_tags()
    lambda_metric(
        "pipeline.gtfs.bundles_downloaded", downloads_count, tags=tags
    )
    lambda_metric(
        "pipeline.gtfs.check_duration_seconds", total_duration, tags=tags
    )


def collate_amtraker_data_for_date(
    year: int, month: int, day: int, mode: Provider | str = Provider.AMTRAK
) -> list[dict]:
    """
    Collect all gzipped JSON data for a specified day from S3.

    Downloads and decompresses all JSON event files stored in S3 for a given
    date and provider, returning them as a list of event dictionaries.

    Parameters
    ----------
    year : int
        Four-digit year (e.g., 2025).
    month : int
        Month number (1-12).
    day : int
        Day of month (1-31).
    mode : Provider or str, default Provider.AMTRAK
        Transit provider to collate data for.

    Returns
    -------
    list of dict
        List of event dictionaries from all JSON files for that day.
        Each dict contains event fields like service_date, route_id,
        trip_id, direction_id, stop_id, event_type, event_time, etc.

    Notes
    -----
    The S3 path structure is:
    ``Events-live/raw/{Provider}/Year={year}/Month={mm}/Day={dd}/``

    Invalid JSON files are skipped with a warning logged.

    Examples
    --------
    >>> events = collate_amtraker_data_for_date(2025, 11, 15, Provider.AMTRAK)
    >>> len(events)
    1500
    """
    date_str = f"{year}-{month:02d}-{day:02d}"
    logger.info(f"Collating data for {mode} on {date_str}")

    # Construct the S3 prefix for the specified day
    # Convert mode to string if it's a Provider enum
    mode_str = str(mode)
    prefix = f"Events-live/raw/{mode_str}/Year={year}/Month={month:02d}/Day={day:02d}/"
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
            tags = get_dd_tags(provider=mode_str.lower())
            lambda_metric("pipeline.collation.raw_files_found", 0, tags=tags)
            lambda_metric(
                "pipeline.collation.missing_day_detected", 1, tags=tags
            )
            return all_events

        file_count = len(response["Contents"])
        json_file_count = sum(
            1
            for obj in response["Contents"]
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

            # Skip empty files
            if decompressed_size == 0:
                logger.debug(f"Skipping empty file: {key}")
                continue

            # Parse JSON
            try:
                decoded_data = decompressed_data.decode("utf-8").strip()
                if not decoded_data:
                    logger.debug(f"Skipping file with empty content: {key}")
                    continue
                events = json.loads(decoded_data)
            except json.JSONDecodeError as e:
                logger.warning(f"Skipping file with invalid JSON ({key}): {e}")
                continue

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

        tags = get_dd_tags(provider=mode_str.lower())
        lambda_metric(
            "pipeline.collation.raw_files_found",
            json_file_count,
            tags=tags,
        )
        lambda_metric(
            "pipeline.collation.events_from_s3",
            len(all_events),
            tags=tags,
        )
        lambda_metric(
            "pipeline.collation.list_duration_seconds",
            list_duration,
            tags=tags,
        )
        lambda_metric(
            "pipeline.collation.process_duration_seconds",
            process_duration,
            tags=tags,
        )

    except Exception as e:
        logger.error(
            f"Error collating data for {mode} on {date_str}: {e}",
            exc_info=True,
        )
        raise

    return all_events


def collate_amtraker_data(
    year: int | None = None,
    month: int | None = None,
    day: int | None = None,
    provider: Provider | str | None = None,
) -> dict:
    """
    Collate daily event data, write to CSV files, and upload to S3.

    This function orchestrates the daily data collation process:

    1. Downloads all raw JSON event files from S3 for the specified date
    2. Writes events to CSV files organized by route/direction/stop
    3. Compresses and uploads CSV files back to S3

    Parameters
    ----------
    year : int, optional
        Four-digit year. If None, uses yesterday's date.
    month : int, optional
        Month number (1-12). If None, uses yesterday's date.
    day : int, optional
        Day of month (1-31). If None, uses yesterday's date.
    provider : Provider or str, optional
        Specific provider to collate. If None, processes all enabled providers.

    Returns
    -------
    dict
        Summary with keys:
        - ``events_count``: Total number of events processed
        - ``files_uploaded``: Number of CSV files uploaded to S3

    Notes
    -----
    The output CSV files are organized in S3 as:
    ``Events-live/daily-{Provider}-data/{route}_{direction}_{stop}/Year=.../``

    Examples
    --------
    Collate yesterday's data for all providers:

    >>> result = collate_amtraker_data()
    >>> print(f"Processed {result['events_count']} events")

    Collate specific date for Amtrak only:

    >>> result = collate_amtraker_data(2025, 11, 15, Provider.AMTRAK)
    """
    start_time = time.time()
    logger.info("Starting data collation")

    # Use yesterday's date if not specified (UTC to avoid timezone issues)
    if year is None or month is None or day is None:
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        year = year or yesterday.year
        month = month or yesterday.month
        day = day or yesterday.day

    logger.info(f"Collating data for date: {year}-{month:02d}-{day:02d}")

    # Determine which providers to process
    if provider is not None:
        # Convert string to Provider if needed
        if isinstance(provider, str):
            try:
                provider = Provider(provider)
            except ValueError:
                pass  # Keep as string if not a valid Provider
        providers = [provider]
    else:
        # Process all enabled providers
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
            provider_str = str(provider)
            for fp in files_for_day:
                if f"daily-{provider_str}-data" in fp:
                    _compress_and_upload_file(fp)
                    uploaded_count += 1
            upload_duration = time.time() - upload_start

            logger.info(
                f"{provider} collation completed - "
                f"{len(events)} events, {uploaded_count} files uploaded "
                f"(upload took {upload_duration:.2f}s)"
            )

            total_events += len(events)
            total_uploads += uploaded_count

        except Exception as e:
            logger.error(
                f"Error collating {provider} data: {e}", exc_info=True
            )

    total_duration = time.time() - start_time
    logger.info(
        f"Data collation completed in {total_duration:.2f}s - "
        f"{total_events} total events, {total_uploads} files uploaded"
    )

    tags = get_dd_tags()
    lambda_metric(
        "pipeline.collation.total_events_processed", total_events, tags=tags
    )
    lambda_metric(
        "pipeline.collation.files_uploaded", total_uploads, tags=tags
    )
    lambda_metric(
        "pipeline.collation.duration_seconds", total_duration, tags=tags
    )

    return {
        "events_count": total_events,
        "files_uploaded": total_uploads,
    }


if __name__ == "__main__":
    generate_event_data()
    check_gtfs_bundle_loop()
