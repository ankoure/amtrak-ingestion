Getting Started
===============

This guide will help you set up the Amtrak Ingestion project for local
development and deployment.

Prerequisites
-------------

* Python 3.12 or higher
* AWS account with appropriate permissions
* AWS CLI configured with credentials
* Access to an S3 bucket for data storage

Installation
------------

1. Clone the repository::

    git clone https://github.com/transitmatters/amtrak-ingestion.git
    cd amtrak-ingestion

2. Create a virtual environment::

    python -m venv .venv
    source .venv/bin/activate  # On Windows: .venv\Scripts\activate

3. Install dependencies::

    pip install -e ".[dev]"

4. Install Chalice CLI (if not already installed)::

    pip install chalice

Environment Configuration
-------------------------

Create a ``.env`` file in the project root with the following variables:

.. code-block:: bash

    AWS_REGION=us-east-1
    S3_BUCKET=amtrak-performance

AWS Permissions
---------------

The Lambda functions require the following IAM permissions:

* ``s3:GetObject`` - Read from S3 bucket
* ``s3:PutObject`` - Write to S3 bucket
* ``s3:ListBucket`` - List bucket contents
* ``logs:CreateLogGroup`` - Create CloudWatch log groups
* ``logs:CreateLogStream`` - Create log streams
* ``logs:PutLogEvents`` - Write log events

Local Development
-----------------

Running Locally
^^^^^^^^^^^^^^^

You can run the Chalice application locally for testing::

    cd amtraker_ingestion
    chalice local

This starts a local server at ``http://localhost:8000``.

Testing Endpoints
^^^^^^^^^^^^^^^^^

Test the health check endpoint::

    curl http://localhost:8000/

Manually trigger GTFS update::

    curl -X POST http://localhost:8000/gtfs/update

Manually trigger data consumption::

    curl -X POST http://localhost:8000/amtraker/update

Running Tests
-------------

Run the test suite::

    pytest

Run with coverage::

    pytest --cov=amtraker_ingestion --cov-report=html

Run specific test markers::

    pytest -m unit      # Unit tests only
    pytest -m integration  # Integration tests only
