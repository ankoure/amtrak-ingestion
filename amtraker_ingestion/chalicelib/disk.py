import csv
import os
import pathlib
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from chalicelib.constants import DATA_DIR, CSV_FIELDS, CSV_FILENAME, Provider


def service_date(ts: datetime) -> date:
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
        writer = csv.DictWriter(fd, fieldnames=CSV_FIELDS, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerow(event)
