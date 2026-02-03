Data Pipeline
=============

This document describes the data processing pipeline in detail.

Overview
--------

The data pipeline transforms raw train data from the Amtraker API into
structured arrival and departure events enriched with GTFS schedule information.

Pipeline Stages
---------------

Stage 1: Data Ingestion
^^^^^^^^^^^^^^^^^^^^^^^

The pipeline begins by fetching data from the Amtraker API.

**Module**: ``read.py``

**Functions**:

* ``validate_amtraker_data()`` - Fetches and validates API response
* ``trainresponse_to_polars()`` - Converts response to Polars DataFrame
* ``read_amtraker_data()`` - Complete ingestion pipeline

**Data Source**:

* Amtraker API: ``https://api-v3.amtraker.com/v3/trains``

**Output**: Polars DataFrame with train and station data

Stage 2: Data Transformation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Raw data is cleaned and restructured for processing.

**Module**: ``read.py``

**Functions**:

* ``remove_excess_fields()`` - Removes unnecessary columns
* ``explode_df()`` - Expands nested station arrays
* ``remove_bus()`` - Filters out bus services
* ``split_df_by_provider()`` - Separates data by transit provider

**Transformations**:

1. Remove metadata fields not needed for analysis
2. Explode station list into individual rows
3. Filter out non-rail services
4. Split DataFrame by provider (Amtrak, VIA, Brightline)

Stage 3: GTFS Enrichment
^^^^^^^^^^^^^^^^^^^^^^^^

Train data is enriched with GTFS schedule information.

**Module**: ``transform.py``

**Functions**:

* ``add_direction_id()`` - Adds GTFS direction IDs
* ``add_scheduled_metrics()`` - Adds scheduled headway and travel time

**Enrichment Process**:

1. **Direction ID**: Look up direction (0/1) based on route and headsign
2. **Scheduled Headway**: Time gap between consecutive vehicles at a stop
3. **Scheduled Travel Time**: Expected time from trip start to each stop

Stage 4: Event Generation
^^^^^^^^^^^^^^^^^^^^^^^^^

Arrival and departure events are generated from the enriched data.

**Module**: ``write.py``

**Functions**:

* ``calculate_service_date_from_datetime()`` - Calculates service date
* ``add_service_dates()`` - Adds service date columns
* ``write_amtraker_events()`` - Generates events

**Event Types**:

* ``ARR`` - Arrival event (when train arrives at station)
* ``DEP`` - Departure event (when train departs from station)

**Service Date Logic**:

The service date is the date the trip started, not the calendar date.
Events between midnight and 3:30 AM are assigned to the previous day's
service date.

Stage 5: Time Filtering
^^^^^^^^^^^^^^^^^^^^^^^

Only new events are processed to avoid duplicates.

**Module**: ``timefilter.py``

**Functions**:

* ``filter_events()`` - Filters by last processed timestamp
* ``get_last_processed()`` - Retrieves last timestamp from S3
* ``set_last_processed()`` - Stores current timestamp in S3

**Filtering Logic**:

Events are filtered based on the ``lastValTS`` field, which indicates
when the train data was last updated. Only events newer than the last
processed timestamp are included.

Stage 6: Storage
^^^^^^^^^^^^^^^^

Processed events are stored in AWS S3.

**Module**: ``s3_upload.py``

**Functions**:

* ``_compress_and_upload_file()`` - Compresses and uploads files
* ``upload_todays_events_to_s3()`` - Uploads daily event files

**Storage Format**:

* JSON format with gzip compression
* Organized by provider, year, month, day, and timestamp

Event Schema
------------

Each generated event contains the following fields:

.. list-table::
   :header-rows: 1
   :widths: 20 15 65

   * - Field
     - Type
     - Description
   * - ``service_date``
     - date
     - The service date for this event
   * - ``route_id``
     - string
     - The route identifier
   * - ``trip_id``
     - string
     - Unique trip identifier
   * - ``direction_id``
     - integer
     - Direction of travel (0 or 1)
   * - ``stop_id``
     - string
     - Station identifier
   * - ``stop_sequence``
     - integer
     - Order of stop in the trip
   * - ``vehicle_id``
     - string
     - Unique vehicle identifier
   * - ``vehicle_label``
     - string
     - Human-readable vehicle label
   * - ``event_type``
     - string
     - "ARR" for arrival, "DEP" for departure
   * - ``event_time``
     - datetime
     - Timestamp of the event
   * - ``scheduled_headway``
     - integer
     - Expected time between vehicles (seconds)
   * - ``scheduled_tt``
     - integer
     - Expected travel time from trip start (seconds)

GTFS Metrics
------------

Scheduled Headway
^^^^^^^^^^^^^^^^^

The scheduled headway is the expected time gap between consecutive vehicles
at a specific stop on a route.

**Calculation**:

1. Load GTFS stop_times for the route
2. Group by stop_id and direction_id
3. Sort by arrival_time
4. Calculate difference between consecutive arrivals

**Usage**:

Headway is used to analyze service frequency and identify gaps in service.

Scheduled Travel Time
^^^^^^^^^^^^^^^^^^^^^

The scheduled travel time is the expected elapsed time from the start of
a trip to a specific stop.

**Calculation**:

1. Load GTFS stop_times for the trip
2. Get arrival time at each stop
3. Calculate difference from first stop's departure time

**Usage**:

Travel time is used to measure on-time performance and delays.

Provider Configuration
----------------------

The pipeline supports multiple transit providers:

.. list-table::
   :header-rows: 1
   :widths: 20 20 60

   * - Provider
     - Enabled
     - Description
   * - Amtrak
     - Yes
     - National passenger railroad service
   * - VIA
     - Yes
     - VIA Rail Canada
   * - Brightline
     - No
     - Florida high-speed rail (disabled)

To enable or disable providers, modify the flags in ``config.py``.

Data Quality
------------

Validation
^^^^^^^^^^

* Pydantic models validate API responses
* Missing required fields raise validation errors
* Invalid data types are rejected

Filtering
^^^^^^^^^

* Bus services are filtered out
* Disabled providers are excluded
* Events older than the last processed time are skipped

Error Handling
^^^^^^^^^^^^^^

* API failures are logged and the pipeline continues
* Invalid records are skipped with warnings
* S3 upload failures trigger retries
