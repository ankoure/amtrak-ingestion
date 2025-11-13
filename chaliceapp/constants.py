from zoneinfo import ZoneInfo
import pathlib

AMTRAKER_API = "https://api-v3.amtraker.com/v3/trains"
AMTRAK_STATIC_GTFS = "https://content.amtrak.com/content/gtfs/GTFS.zip"
BRIGHTLINE_STATIC_GTFS = "http://feed.gobrightline.com/bl_gtfs.zip"
VIA_RAIL_STATIC_GTFS = "https://www.viarail.ca/sites/all/files/gtfs/viarail.zip"
EASTERN_TIME = ZoneInfo("US/Eastern")
S3_BUCKET = "amtrak-performance"
DATA_DIR = pathlib.Path("data")
LOCAL_DATA_TEMPLATE = str(
    DATA_DIR / "daily-*/*/Year={year}/Month={month}/Day={day}/events.csv"
)
S3_DATA_TEMPLATE = "Events-live/{relative_path}.gz"
CSV_FILENAME = "events.csv"
CSV_FIELDS = [
    "service_date",
    "route_id",
    "trip_id",
    "direction_id",
    "stop_id",
    "stop_sequence",
    "vehicle_id",
    "vehicle_label",
    "event_type",
    "event_time",
    "scheduled_headway",
    "scheduled_tt",
]
