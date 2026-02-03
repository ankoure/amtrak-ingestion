API Endpoints
=============

The Amtrak Ingestion service exposes HTTP endpoints for health checks and
manual triggering of pipeline operations.

HTTP Endpoints
--------------

Health Check
^^^^^^^^^^^^

.. http:get:: /

   Returns a simple health check response.

   **Response**:

   .. code-block:: json

      {"hello": "world"}

   **Status Codes**:

   * ``200 OK`` - Service is healthy

GTFS Update
^^^^^^^^^^^

.. http:post:: /gtfs/update

   Manually triggers an update of the GTFS cache for all enabled providers.

   This endpoint checks the last modified date of GTFS feeds and downloads
   updated bundles if available.

   **Response**:

   .. code-block:: json

      {"message": "GTFS update completed"}

   **Status Codes**:

   * ``200 OK`` - Update completed (may or may not have downloaded new data)

Amtraker Update
^^^^^^^^^^^^^^^

.. http:post:: /amtraker/update

   Manually triggers consumption of the Amtraker API and event generation.

   This endpoint:

   1. Fetches current train data from the Amtraker API
   2. Validates and transforms the data
   3. Enriches with GTFS metrics
   4. Generates arrival/departure events
   5. Uploads events to S3

   **Response**:

   .. code-block:: json

      {"message": "Amtraker update completed"}

   **Status Codes**:

   * ``200 OK`` - Update completed successfully

Data Collation
^^^^^^^^^^^^^^

.. http:post:: /amtraker/collate

   Manually triggers data collation for a specific date.

   **Query Parameters**:

   * ``year`` (required): Four-digit year (e.g., ``2025``)
   * ``month`` (required): Two-digit month (e.g., ``01``)
   * ``day`` (required): Two-digit day (e.g., ``15``)

   **Example Request**::

       POST /amtraker/collate?year=2025&month=11&day=15

   **Response**:

   .. code-block:: json

      {"message": "Collation completed"}

   **Status Codes**:

   * ``200 OK`` - Collation completed successfully

Scheduled Functions
-------------------

The following functions are triggered automatically by AWS EventBridge:

Update GTFS Cache
^^^^^^^^^^^^^^^^^

* **Schedule**: Daily at 2:00 AM UTC
* **Function**: ``update_gtfs_cache``
* **Description**: Checks and updates GTFS bundles from all enabled providers

Consume Amtraker API
^^^^^^^^^^^^^^^^^^^^

* **Schedule**: Every 5 minutes (``rate(5 minutes)``)
* **Function**: ``consume_amtraker_api``
* **Description**: Fetches train data, generates events, uploads to S3

Collate Previous Day
^^^^^^^^^^^^^^^^^^^^

* **Schedule**: Daily at 3:00 AM UTC
* **Function**: ``collate_previous_day``
* **Description**: Collates all events from the previous day into CSV files

Cron Expression Reference
-------------------------

The scheduled functions use AWS cron expressions:

.. code-block:: text

    # Daily at 2:00 AM UTC
    cron(0 2 * * ? *)

    # Every 5 minutes
    rate(5 minutes)

    # Daily at 3:00 AM UTC
    cron(0 3 * * ? *)

Error Responses
---------------

All endpoints may return the following error responses:

.. code-block:: json

   {
     "Code": "InternalServerError",
     "Message": "An internal server error occurred"
   }

**Status Codes**:

* ``500 Internal Server Error`` - Unexpected error during processing

Usage Examples
--------------

Using curl
^^^^^^^^^^

Health check::

    curl https://your-api-gateway-url.amazonaws.com/api/

Trigger GTFS update::

    curl -X POST https://your-api-gateway-url.amazonaws.com/api/gtfs/update

Trigger Amtraker update::

    curl -X POST https://your-api-gateway-url.amazonaws.com/api/amtraker/update

Collate specific date::

    curl -X POST "https://your-api-gateway-url.amazonaws.com/api/amtraker/collate?year=2025&month=11&day=15"

Using Python
^^^^^^^^^^^^

.. code-block:: python

    import requests

    BASE_URL = "https://your-api-gateway-url.amazonaws.com/api"

    # Health check
    response = requests.get(f"{BASE_URL}/")
    print(response.json())

    # Trigger GTFS update
    response = requests.post(f"{BASE_URL}/gtfs/update")
    print(response.json())

    # Collate specific date
    params = {"year": "2025", "month": "11", "day": "15"}
    response = requests.post(f"{BASE_URL}/amtraker/collate", params=params)
    print(response.json())
