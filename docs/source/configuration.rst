Configuration
=============

This document describes the configuration options for the Amtrak Ingestion system.

Environment Variables
---------------------

The following environment variables can be set to configure the application:

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Variable
     - Required
     - Description
   * - ``AWS_REGION``
     - No
     - AWS region for S3 and Lambda (default: us-east-1)
   * - ``AWS_ACCESS_KEY_ID``
     - No
     - AWS access key (use IAM roles in production)
   * - ``AWS_SECRET_ACCESS_KEY``
     - No
     - AWS secret key (use IAM roles in production)

Provider Configuration
----------------------

Provider enablement is configured in ``chalicelib/config.py``:

.. code-block:: python

    # Provider enablement flags
    AMTRAK_ENABLED = True
    VIA_ENABLED = True
    BRIGHTLINE_ENABLED = False

To enable or disable a provider:

1. Open ``chalicelib/config.py``
2. Set the corresponding flag to ``True`` or ``False``
3. Redeploy the application

Constants
---------

Key constants are defined in ``chalicelib/constants.py``:

S3 Bucket
^^^^^^^^^

.. code-block:: python

    S3_BUCKET = "amtrak-performance"

The S3 bucket where all data is stored. Change this to use a different bucket.

API Endpoints
^^^^^^^^^^^^^

.. code-block:: python

    AMTRAKER_TRAINS_API = "https://api-v3.amtraker.com/v3/trains"

    GTFS_URLS = {
        Provider.AMTRAK: "https://content.amtrak.com/content/gtfs/GTFS.zip",
        Provider.VIA: "https://www.viarail.ca/sites/all/files/gtfs/viarail.zip",
        Provider.BRIGHTLINE: "https://www.gobrightline.com/gtfs/gtfs.zip",
    }

Timezone
^^^^^^^^

.. code-block:: python

    EASTERN = ZoneInfo("America/New_York")

The primary timezone for service date calculations.

CSV Fields
^^^^^^^^^^

.. code-block:: python

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

Fields written to the collated CSV files.

Logging Configuration
---------------------

Logging is configured in ``chalicelib/config.py``:

.. code-block:: python

    def setup_logging():
        """Configure logging for the application."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

Log Levels
^^^^^^^^^^

* ``DEBUG`` - Detailed debugging information
* ``INFO`` - General operational information
* ``WARNING`` - Warning messages for potential issues
* ``ERROR`` - Error messages for failures

Getting a Logger
^^^^^^^^^^^^^^^^

.. code-block:: python

    from chalicelib.config import get_logger

    logger = get_logger(__name__)
    logger.info("Processing started")

Chalice Configuration
---------------------

Chalice configuration is stored in ``amtraker_ingestion/.chalice/config.json``:

.. code-block:: json

    {
      "version": "2.0",
      "app_name": "amtraker-ingestion",
      "stages": {
        "dev": {
          "api_gateway_stage": "api"
        }
      }
    }

Lambda Settings
^^^^^^^^^^^^^^^

Configure Lambda function settings in the Chalice config:

.. code-block:: json

    {
      "stages": {
        "prod": {
          "lambda_memory_size": 512,
          "lambda_timeout": 300,
          "environment_variables": {
            "LOG_LEVEL": "INFO"
          }
        }
      }
    }

IAM Policy
^^^^^^^^^^

The IAM policy is defined in ``.chalice/policy-dev.json``:

.. code-block:: json

    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Action": [
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents"
          ],
          "Resource": "arn:aws:logs:*:*:*"
        },
        {
          "Effect": "Allow",
          "Action": [
            "s3:GetObject",
            "s3:PutObject",
            "s3:ListBucket"
          ],
          "Resource": [
            "arn:aws:s3:::amtrak-performance",
            "arn:aws:s3:::amtrak-performance/*"
          ]
        }
      ]
    }

Schedule Configuration
----------------------

Scheduled tasks are defined in ``app.py`` using Chalice decorators:

.. code-block:: python

    @app.schedule(Cron(0, 2, "*", "*", "?", "*"))
    def update_gtfs_cache(event):
        """Run daily at 2:00 AM UTC."""
        pass

    @app.schedule(Rate(5, unit=Rate.MINUTES))
    def consume_amtraker_api(event):
        """Run every 5 minutes."""
        pass

    @app.schedule(Cron(0, 3, "*", "*", "?", "*"))
    def collate_previous_day(event):
        """Run daily at 3:00 AM UTC."""
        pass

Customizing Schedules
^^^^^^^^^^^^^^^^^^^^^

Cron expressions follow AWS EventBridge syntax:

.. code-block:: text

    Cron(minutes, hours, day_of_month, month, day_of_week, year)

Examples:

* ``Cron(0, 2, "*", "*", "?", "*")`` - Daily at 2:00 AM
* ``Cron(30, 8, "*", "*", "?", "*")`` - Daily at 8:30 AM
* ``Cron(0, "*/6", "*", "*", "?", "*")`` - Every 6 hours

Rate expressions:

* ``Rate(5, unit=Rate.MINUTES)`` - Every 5 minutes
* ``Rate(1, unit=Rate.HOURS)`` - Every hour
* ``Rate(1, unit=Rate.DAYS)`` - Every day
