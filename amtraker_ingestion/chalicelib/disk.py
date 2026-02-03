"""
Local Disk Storage Module
=========================

This module handles local file storage for event data, including
service date calculation and CSV file writing.

Main Functions
--------------
service_date
    Calculate the service date from a timestamp
output_dir_path
    Generate the local output directory path for events
write_event
    Write an event to a CSV file
"""

import csv
import os
import pathlib
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from chalicelib.constants import DATA_DIR, CSV_FIELDS, CSV_FILENAME, Provider


def service_date(ts: datetime) -> date:
    """
    Calculate the service date from a timestamp.

    The service date is the operational date of a transit trip, which may
    differ from the calendar date for late-night services. Events between
    midnight and 3:00 AM are assigned to the previous day's service date.

    Parameters
    ----------
    ts : datetime
        Timestamp to calculate service date for. Assumed to be UTC if
        no timezone is specified.

    Returns
    -------
    date
        The service date for the given timestamp.

    Notes
    -----
    This follows transit industry conventions where trips starting before
    midnight but ending after midnight belong to the previous day's service.
    """
    # In practice a None TZ is UTC, but we want to be explicit
    # In many places we have an implied eastern
    ts = ts.replace(tzinfo=ZoneInfo("UTC"))

    if ts.hour >= 3 and ts.hour <= 23:
        return date(ts.year, ts.month, ts.day)

    prior = ts - timedelta(days=1)
    return date(prior.year, prior.month, prior.day)


def output_dir_path(
    route_id: str,
    direction_id: str,
    stop_id: str,
    ts: datetime,
    mode: Provider | str = Provider.AMTRAK,
) -> str:
    """
    Generate the local output directory path for event files.

    Creates a hierarchical path structure organized by provider, route,
    direction, stop, and date for efficient data organization.

    Parameters
    ----------
    route_id : str
        Route identifier (e.g., "Northeast Regional").
    direction_id : str
        Direction of travel (0 or 1).
    stop_id : str
        Station code (e.g., "NYP" for New York Penn).
    ts : datetime
        Timestamp for calculating service date.
    mode : Provider or str, default Provider.AMTRAK
        Transit provider name.

    Returns
    -------
    str
        Relative path like
        ``daily-Amtrak-data/route_0_NYP/Year=2025/Month=11/Day=15``.
    """
    date = service_date(ts)
    delimiter = "_"
    stop_path = f"{route_id}{delimiter}{direction_id}{delimiter}{stop_id}"

    return os.path.join(
        f"daily-{mode}-data",
        stop_path,
        f"Year={date.year}",
        f"Month={date.month}",
        f"Day={date.day}",
    )


def write_event(event: dict, mode: Provider | str = Provider.AMTRAK):
    """
    Write a single event to a CSV file.

    Appends the event to the appropriate CSV file based on route, direction,
    stop, and service date. Creates the directory structure and CSV header
    if the file doesn't exist.

    Parameters
    ----------
    event : dict
        Event dictionary containing required fields: route_id, direction_id,
        stop_id, event_time, event_type, service_date, trip_id, vehicle_id,
        vehicle_label, scheduled_headway, scheduled_tt.
    mode : Provider or str, default Provider.AMTRAK
        Transit provider name for directory organization.

    Notes
    -----
    The CSV file is written to:
    ``{DATA_DIR}/daily-{mode}-data/{route}_{dir}_{stop}/Year=.../events.csv``
    """
    # Convert event_time to datetime if it's a string
    event_time = event["event_time"]
    if isinstance(event_time, str):
        event_time = datetime.fromisoformat(event_time.replace("Z", "+00:00"))

    dirname = DATA_DIR / pathlib.Path(
        output_dir_path(
            event["route_id"],
            event["direction_id"],
            event["stop_id"],
            event_time,
            mode,
        )
    )
    dirname.mkdir(parents=True, exist_ok=True)
    pathname = dirname / CSV_FILENAME
    file_exists = os.path.isfile(pathname)
    with pathname.open("a") as fd:
        writer = csv.DictWriter(
            fd, fieldnames=CSV_FIELDS, extrasaction="ignore"
        )
        if not file_exists:
            writer.writeheader()
        writer.writerow(event)
