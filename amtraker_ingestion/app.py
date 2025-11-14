from chalice.app import Chalice, Cron
from chalicelib.main import (
    check_gtfs_bundle_loop,
    generate_event_data,
    collate_amtraker_data as collate_previous_day_data,
)
from chalicelib.main import collate_amtraker_data_for_date
from chalicelib.constants import Provider

app = Chalice(app_name="amtrak-ingestion")


@app.route("/")
def index():
    return {"hello": "world"}


@app.schedule(Cron(0, 2, "*", "*", "?", "*"))
def update_gtfs_cache(event):
    """
    Scheduled function to check for and update GTFS bundles in S3
    Runs daily at 2:00 AM UTC

    The Cron format is: Cron(minutes, hours, day_of_month, month, day_of_week, year)
    Current: 0 2 * * ? * = Every day at 2:00 AM UTC
    """
    check_gtfs_bundle_loop()


@app.route("/gtfs/update", methods=["POST"])
def manual_gtfs_update():
    """
    Manual endpoint to trigger GTFS cache update
    Useful for testing or forcing an update outside the schedule
    """
    check_gtfs_bundle_loop()

    return {"status": "completed"}


@app.schedule(Cron("*/5", "*", "*", "*", "?", "*"))
def consume_amtraker_api(event):
    """
    Scheduled function to fetch train data from Amtraker API and generate events
    Runs every 5 minutes

    The Cron format is: Cron(minutes, hours, day_of_month, month, day_of_week, year)
    Current: */5 * * * ? * = Every 5 minutes
    """
    generate_event_data()


@app.route("/amtraker/update", methods=["POST"])
def manual_amtraker_update():
    """
    Manual endpoint to trigger Amtraker data ingestion
    Useful for testing or forcing an update outside the schedule
    """
    generate_event_data()

    return {"status": "completed"}


@app.schedule(Cron(0, 3, "*", "*", "?", "*"))
def collate_previous_day(event):
    """
    Scheduled function to collate previous day's data for all providers
    Runs daily at 3:00 AM UTC (which gives time for all data to be collected)

    The Cron format is: Cron(minutes, hours, day_of_month, month, day_of_week, year)
    Current: 0 3 * * ? * = Every day at 3:00 AM UTC
    """
    collate_previous_day_data()


@app.route("/amtraker/collate", methods=["POST"])
def manual_collate_amtraker_data():
    """
    Manual endpoint to collate Amtraker data for a specified day or previous day

    Expects JSON body (optional):
    {
        "year": 2025,      // optional - if not provided, collates previous day
        "month": 1,        // optional - if not provided, collates previous day
        "day": 15,         // optional - if not provided, collates previous day
        "mode": "amtrak"   // optional, defaults to "Amtrak" (only used if date specified)
    }

    If no parameters provided, collates previous day's data for all providers.
    """

    request = app.current_request
    params = request.json_body if request.json_body else {}

    year = params.get("year")
    month = params.get("month")
    day = params.get("day")
    mode_str = params.get("mode", "Amtrak")

    # Convert string to Provider enum if valid
    try:
        mode = Provider(mode_str)
    except ValueError:
        mode = mode_str  # Keep as string if not a valid Provider

    # If no date specified, collate previous day for all providers
    if not all([year, month, day]):
        collate_previous_day_data()
        return {
            "status": "completed",
            "message": "Collated previous day's data for all providers",
        }

    # If date specified, collate for that specific date and provider
    events = collate_amtraker_data_for_date(year, month, day, mode)

    return {
        "status": "completed",
        "events_count": len(events),
        "year": year,
        "month": month,
        "day": day,
        "mode": mode,
    }


# The view function above will return {"hello": "world"}
# whenever you make an HTTP GET request to '/'.
#
# Here are a few more examples:
#
# @app.route('/hello/{name}')
# def hello_name(name):
#    # '/hello/james' -> {"hello": "james"}
#    return {'hello': name}
#
# @app.route('/users', methods=['POST'])
# def create_user():
#     # This is the JSON body the user sent in their POST request.
#     user_as_json = app.current_request.json_body
#     # We'll echo the json body back to the user in a 'user' key.
#     return {'user': user_as_json}
#
# See the README documentation for more examples.
#
