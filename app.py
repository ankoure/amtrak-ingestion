from chalice import Chalice, Cron
from chaliceapp.main import check_gtfs_bundle_loop, generate_event_data

app = Chalice(app_name="test")


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
