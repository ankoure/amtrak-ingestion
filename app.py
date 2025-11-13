from chalice import Chalice, Cron
from chaliceapp.main import check_gtfs_bundle_loop

app = Chalice(app_name="test")


@app.route("/")
def index():
    return {"hello": "world"}


@app.schedule(Cron(0, 2, "*", "*", "?", "*"))
def update_gtfs_cache(event):
    """
    Scheduled function to update GTFS caches in S3
    Runs daily at 2:00 AM UTC (adjust as needed)

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
    from chaliceapp.gtfs_cache import update_all_gtfs_caches

    results = update_all_gtfs_caches()
    return {"status": "completed", "results": results}


@app.route("/gtfs/metadata/{provider}")
def get_gtfs_info(provider):
    """
    Get metadata about a cached GTFS bundle

    Args:
        provider: Provider name (amtrak, via_rail, or brightline)
    """
    from chaliceapp.gtfs_cache import get_gtfs_metadata

    metadata = get_gtfs_metadata(provider)
    if metadata:
        return metadata
    else:
        return {"error": f"No cached GTFS found for provider: {provider}"}, 404


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
