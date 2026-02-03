Amtrak Ingestion Documentation
==============================

Welcome to the Amtrak Ingestion documentation. This project is a serverless data
pipeline built on AWS Chalice that ingests, enriches, and processes real-time
train data from multiple transit providers (Amtrak, VIA Rail, and Brightline).

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   getting_started
   architecture
   api_endpoints
   data_pipeline
   configuration
   deployment
   api_reference

Features
--------

* Real-time train data ingestion from Amtraker API
* GTFS data enrichment for scheduled metrics
* Multi-provider support (Amtrak, VIA Rail, Brightline)
* Automated event generation for arrivals and departures
* AWS S3 storage with efficient compression
* Daily data collation and aggregation
* Serverless architecture using AWS Lambda

Quick Start
-----------

1. Install dependencies::

    pip install -e ".[dev]"

2. Configure AWS credentials and environment variables

3. Deploy to AWS::

    cd amtraker_ingestion
    chalice deploy

4. The pipeline will automatically:

   * Update GTFS cache daily at 2:00 AM UTC
   * Consume Amtraker API every 5 minutes
   * Collate previous day's data at 3:00 AM UTC

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
