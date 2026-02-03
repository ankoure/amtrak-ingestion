Architecture
============

This document describes the system architecture of the Amtrak Ingestion pipeline.

Overview
--------

The Amtrak Ingestion system is a serverless data pipeline built on AWS Chalice
that processes real-time train data through several stages:

1. **Data Ingestion** - Fetch data from external APIs
2. **Data Enrichment** - Add GTFS metrics and direction information
3. **Event Generation** - Generate arrival/departure events
4. **Data Storage** - Store processed data in S3
5. **Data Collation** - Aggregate daily data for analysis

System Components
-----------------

.. code-block:: text

    +------------------+     +------------------+     +------------------+
    |   Amtraker API   |     |    GTFS Feeds    |     |   AWS EventBridge|
    +--------+---------+     +--------+---------+     +--------+---------+
             |                        |                        |
             v                        v                        v
    +--------+---------+     +--------+---------+     +--------+---------+
    |    read.py       |     |    gtfs.py       |     | Scheduled Tasks  |
    | (Data Reading)   |     | (GTFS Processing)|     | (Cron Triggers)  |
    +--------+---------+     +--------+---------+     +--------+---------+
             |                        |                        |
             +------------+-----------+------------------------+
                          |
                          v
             +------------+------------+
             |       main.py           |
             |  (Pipeline Orchestration)|
             +------------+------------+
                          |
            +-------------+-------------+
            |                           |
            v                           v
    +-------+--------+         +--------+-------+
    |  transform.py  |         |    write.py    |
    | (Enrichment)   |         | (Event Gen)    |
    +-------+--------+         +--------+-------+
            |                           |
            +-------------+-------------+
                          |
                          v
             +------------+------------+
             |       disk.py           |
             |   (Local File Storage)  |
             +------------+------------+
                          |
                          v
             +------------+------------+
             |     s3_upload.py        |
             |    (S3 Operations)      |
             +------------+------------+
                          |
                          v
             +------------+------------+
             |        AWS S3           |
             |   (Data Persistence)    |
             +-------------------------+

Data Flow
---------

Real-time Data Pipeline
^^^^^^^^^^^^^^^^^^^^^^^

The real-time pipeline runs every 5 minutes:

1. **Fetch Data**: ``read.py`` fetches data from the Amtraker API
2. **Validate**: Pydantic models validate the API response
3. **Transform**: Convert to Polars DataFrame and split by provider
4. **Enrich**: ``transform.py`` adds GTFS direction IDs and scheduled metrics
5. **Filter**: ``timefilter.py`` filters to only new events
6. **Generate Events**: ``write.py`` creates arrival/departure records
7. **Store**: Upload compressed JSON to S3

Daily Collation Pipeline
^^^^^^^^^^^^^^^^^^^^^^^^

The collation pipeline runs daily at 3:00 AM UTC:

1. **Download**: Fetch all gzipped JSON files for the previous day from S3
2. **Decompress**: Extract JSON data from gzip archives
3. **Process**: Parse events and organize by route/direction/stop
4. **Write CSV**: Generate CSV files with event data
5. **Compress**: Gzip CSV files for storage efficiency
6. **Upload**: Store collated data in S3

S3 Data Organization
--------------------

Raw Events
^^^^^^^^^^

Real-time event data is stored in a hierarchical structure:

.. code-block:: text

    s3://amtrak-performance/
    └── Events-live/
        └── raw/
            └── {Provider}/
                └── Year={YYYY}/
                    └── Month={MM}/
                        └── Day={DD}/
                            └── _{HH}_{MM}.json.gz

Collated Data
^^^^^^^^^^^^^

Daily collated data is organized by route, direction, and stop:

.. code-block:: text

    s3://amtrak-performance/
    └── Events-live/
        └── daily-{Provider}-data/
            └── {route}_{direction}_{stop}/
                └── Year={YYYY}/
                    └── Month={MM}/
                        └── Day={DD}/
                            └── events.csv.gz

GTFS Cache
^^^^^^^^^^

GTFS bundles are cached in S3:

.. code-block:: text

    s3://amtrak-performance/
    └── GTFS/
        ├── Amtrak.zip
        ├── VIA.zip
        ├── Brightline.zip
        └── last_modified.json

Module Responsibilities
-----------------------

+------------------+--------------------------------------------------+
| Module           | Responsibility                                   |
+==================+==================================================+
| ``app.py``       | Chalice application entry point, HTTP endpoints, |
|                  | scheduled task definitions                       |
+------------------+--------------------------------------------------+
| ``main.py``      | Pipeline orchestration, GTFS bundle management,  |
|                  | data collation                                   |
+------------------+--------------------------------------------------+
| ``read.py``      | API data fetching, validation, DataFrame         |
|                  | transformations                                  |
+------------------+--------------------------------------------------+
| ``transform.py`` | Data enrichment with GTFS direction IDs and      |
|                  | scheduled metrics                                |
+------------------+--------------------------------------------------+
| ``write.py``     | Event generation, service date calculation       |
+------------------+--------------------------------------------------+
| ``gtfs.py``      | GTFS data loading, metrics calculation,          |
|                  | direction lookup generation                      |
+------------------+--------------------------------------------------+
| ``disk.py``      | Local file storage operations                    |
+------------------+--------------------------------------------------+
| ``s3_upload.py`` | S3 upload/download operations                    |
+------------------+--------------------------------------------------+
| ``timefilter.py``| Event filtering by timestamp                     |
+------------------+--------------------------------------------------+
| ``config.py``    | Configuration, logging setup, AWS clients        |
+------------------+--------------------------------------------------+
| ``constants.py`` | Constants, enums, field definitions              |
+------------------+--------------------------------------------------+

Error Handling
--------------

The pipeline implements several error handling strategies:

* **API Failures**: Log errors and continue with next scheduled run
* **Invalid Data**: Pydantic validation rejects malformed responses
* **S3 Errors**: Retry logic with exponential backoff
* **GTFS Issues**: Fall back to cached data if update fails

Logging
-------

All modules use structured logging configured in ``config.py``:

* Log format: ``%(asctime)s | %(levelname)s | %(name)s | %(message)s``
* Date format: ``%Y-%m-%d %H:%M:%S``
* CloudWatch-compatible output
