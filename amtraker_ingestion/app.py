from chalice.app import Chalice, Cron
from chalicelib.main import (
    check_gtfs_bundle_loop,
    generate_event_data,
    collate_amtraker_data,
)
from chalicelib.constants import Provider
from chalicelib.config import get_logger
import time

try:
    from datadog_lambda.wrapper import datadog_lambda_wrapper
except ImportError:

    def datadog_lambda_wrapper(fn):  # type: ignore[misc]
        return fn


app = Chalice(app_name="amtrak-ingestion")
logger = get_logger(__name__)


@app.route("/")
def index():
    logger.info("Health check endpoint called")
    return {"hello": "world"}


@datadog_lambda_wrapper
@app.schedule(Cron(0, 2, "*", "*", "?", "*"))
def update_gtfs_cache(event):
    """
    Scheduled function to check for and update GTFS bundles in S3
    Runs daily at 2:00 AM UTC

    The Cron format is: Cron(minutes, hours, day_of_month,
    month, day_of_week, year)
    Current: 0 2 * * ? * = Every day at 2:00 AM UTC
    """
    logger.info("Scheduled GTFS cache update triggered")
    start_time = time.time()

    try:
        check_gtfs_bundle_loop()
        duration = time.time() - start_time
        logger.info(
            f"Scheduled GTFS cache update completed in {duration:.2f}s"
        )
    except Exception as e:
        logger.error(f"Scheduled GTFS cache update failed: {e}", exc_info=True)
        raise


@app.route("/gtfs/update", methods=["POST"])
def manual_gtfs_update():
    """
    Manual endpoint to trigger GTFS cache update
    Useful for testing or forcing an update outside the schedule
    """
    logger.info("Manual GTFS cache update requested")
    start_time = time.time()

    try:
        check_gtfs_bundle_loop()
        duration = time.time() - start_time
        logger.info(f"Manual GTFS cache update completed in {duration:.2f}s")
        return {"status": "completed", "duration_seconds": duration}
    except Exception as e:
        logger.error(f"Manual GTFS cache update failed: {e}", exc_info=True)
        raise


@datadog_lambda_wrapper
@app.schedule(Cron("*/5", "*", "*", "*", "?", "*"))
def consume_amtraker_api(event):
    """
    Scheduled function to fetch train data from Amtraker API
    and generate events. Runs every 5 minutes

    The Cron format is: Cron(minutes, hours, day_of_month,
    month, day_of_week, year)
    Current: */5 * * * ? * = Every 5 minutes
    """
    logger.info("Scheduled Amtraker API consumption triggered")
    start_time = time.time()

    try:
        generate_event_data()
        duration = time.time() - start_time
        logger.info(
            f"Scheduled Amtraker API consumption completed in {duration:.2f}s"
        )
    except Exception as e:
        logger.error(
            f"Scheduled Amtraker API consumption failed: {e}", exc_info=True
        )
        raise


@app.route("/amtraker/update", methods=["POST"])
def manual_amtraker_update():
    """
    Manual endpoint to trigger Amtraker data ingestion
    Useful for testing or forcing an update outside the schedule
    """
    logger.info("Manual Amtraker update requested")
    start_time = time.time()

    try:
        generate_event_data()
        duration = time.time() - start_time
        logger.info(f"Manual Amtraker update completed in {duration:.2f}s")
        return {"status": "completed", "duration_seconds": duration}
    except Exception as e:
        logger.error(f"Manual Amtraker update failed: {e}", exc_info=True)
        raise


@datadog_lambda_wrapper
@app.schedule(Cron(0, 3, "*", "*", "?", "*"))
def collate_previous_day(event):
    """
    Scheduled function to collate previous day's data for all providers
    Runs daily at 3:00 AM UTC (gives time for all data to be collected)

    The Cron format is: Cron(minutes, hours, day_of_month,
    month, day_of_week, year)
    Current: 0 3 * * ? * = Every day at 3:00 AM UTC
    """
    logger.info("Scheduled data collation triggered")
    start_time = time.time()

    try:
        collate_amtraker_data()
        duration = time.time() - start_time
        logger.info(f"Scheduled data collation completed in {duration:.2f}s")
    except Exception as e:
        logger.error(f"Scheduled data collation failed: {e}", exc_info=True)
        raise


@app.route("/amtraker/collate", methods=["POST"])
def manual_collate_amtraker_data():
    """
    Manual endpoint to collate Amtraker data for a specified day
    or previous day

    Expects JSON body (optional):
    {
        "year": 2025,      // optional - if not provided,
                           // collates previous day
        "month": 1,        // optional
        "day": 15,         // optional
        "mode": "amtrak"   // optional, defaults to "Amtrak"
                           // (only used if date specified)
    }

    If no parameters provided, collates previous day's data for all
    providers.
    """
    logger.info("Manual data collation requested")
    start_time = time.time()

    request = app.current_request
    params = request.json_body if request.json_body else {}

    year = params.get("year")
    month = params.get("month")
    day = params.get("day")
    mode_str = params.get("mode", "Amtrak")

    logger.info(
        f"Collation parameters: year={year}, month={month}, day={day}, mode={mode_str}"
    )

    # Convert string to Provider enum if valid
    try:
        mode = Provider(mode_str)
    except ValueError:
        mode = mode_str  # Keep as string if not a valid Provider

    # If no date specified, collate previous day for all providers
    if not all([year, month, day]):
        logger.info("No date specified, collating previous day")
        try:
            result = collate_amtraker_data()
            duration = time.time() - start_time
            logger.info(
                f"Manual collation (previous day) completed in {duration:.2f}s"
            )
            return {
                "status": "completed",
                "message": "Collated previous day's data for all providers",
                "events_count": result["events_count"],
                "files_uploaded": result["files_uploaded"],
                "duration_seconds": duration,
            }
        except Exception as e:
            logger.error(
                f"Manual collation (previous day) failed: {e}", exc_info=True
            )
            raise

    # If date specified, collate for that specific date and provider
    logger.info(f"Collating data for {year}-{month:02d}-{day:02d} ({mode})")
    try:
        result = collate_amtraker_data(year, month, day, mode)
        duration = time.time() - start_time
        logger.info(
            f"Manual collation for {year}-{month:02d}-{day:02d} "
            f"completed in {duration:.2f}s - {result['events_count']} events, "
            f"{result['files_uploaded']} files uploaded"
        )
        return {
            "status": "completed",
            "events_count": result["events_count"],
            "files_uploaded": result["files_uploaded"],
            "year": year,
            "month": month,
            "day": day,
            "mode": str(mode),
            "duration_seconds": duration,
        }
    except Exception as e:
        logger.error(
            f"Manual collation for {year}-{month:02d}-{day:02d} failed: {e}",
            exc_info=True,
        )
        raise


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
