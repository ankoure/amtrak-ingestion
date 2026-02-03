Deployment
==========

This document describes how to deploy the Amtrak Ingestion system to AWS.

Prerequisites
-------------

Before deploying, ensure you have:

* AWS CLI installed and configured
* Chalice CLI installed (``pip install chalice``)
* AWS credentials with appropriate permissions
* An S3 bucket created for data storage

Deployment Process
------------------

Development Deployment
^^^^^^^^^^^^^^^^^^^^^^

Deploy to the development stage::

    cd amtraker_ingestion
    chalice deploy

This creates:

* API Gateway endpoint
* Lambda functions for HTTP endpoints
* Lambda functions for scheduled tasks
* IAM roles and policies
* CloudWatch log groups

Production Deployment
^^^^^^^^^^^^^^^^^^^^^

Deploy to production with a specific stage::

    cd amtraker_ingestion
    chalice deploy --stage prod

View Deployment
^^^^^^^^^^^^^^^

View deployed resources::

    chalice url

Delete Deployment
^^^^^^^^^^^^^^^^^

Remove all deployed resources::

    chalice delete --stage dev

AWS Resources
-------------

The deployment creates the following AWS resources:

Lambda Functions
^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Function Name
     - Purpose
   * - ``amtraker-ingestion-dev``
     - HTTP API handler
   * - ``amtraker-ingestion-dev-update_gtfs_cache``
     - Daily GTFS cache update
   * - ``amtraker-ingestion-dev-consume_amtraker_api``
     - 5-minute data ingestion
   * - ``amtraker-ingestion-dev-collate_previous_day``
     - Daily data collation

API Gateway
^^^^^^^^^^^

* REST API with endpoints:

  * ``GET /`` - Health check
  * ``POST /gtfs/update`` - Manual GTFS update
  * ``POST /amtraker/update`` - Manual data update
  * ``POST /amtraker/collate`` - Manual collation

EventBridge Rules
^^^^^^^^^^^^^^^^^

* ``amtraker-ingestion-dev-update_gtfs_cache`` - Daily at 2:00 AM UTC
* ``amtraker-ingestion-dev-consume_amtraker_api`` - Every 5 minutes
* ``amtraker-ingestion-dev-collate_previous_day`` - Daily at 3:00 AM UTC

IAM Roles
^^^^^^^^^

* ``amtraker-ingestion-dev`` - Execution role for Lambda functions

Required Permissions
--------------------

The deployment requires the following IAM permissions:

.. code-block:: json

    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Action": [
            "cloudformation:*",
            "iam:*",
            "lambda:*",
            "apigateway:*",
            "events:*",
            "logs:*",
            "s3:*"
          ],
          "Resource": "*"
        }
      ]
    }

.. warning::

   These are broad permissions for deployment. In production, use more
   restrictive policies following the principle of least privilege.

Configuration by Stage
----------------------

Configure different stages in ``.chalice/config.json``:

.. code-block:: json

    {
      "version": "2.0",
      "app_name": "amtraker-ingestion",
      "stages": {
        "dev": {
          "api_gateway_stage": "api",
          "lambda_memory_size": 256,
          "lambda_timeout": 120
        },
        "prod": {
          "api_gateway_stage": "api",
          "lambda_memory_size": 512,
          "lambda_timeout": 300,
          "environment_variables": {
            "LOG_LEVEL": "WARNING"
          }
        }
      }
    }

Monitoring
----------

CloudWatch Logs
^^^^^^^^^^^^^^^

View Lambda logs in CloudWatch:

1. Open AWS CloudWatch Console
2. Navigate to Log Groups
3. Find ``/aws/lambda/amtraker-ingestion-dev-*``

Using AWS CLI::

    aws logs tail /aws/lambda/amtraker-ingestion-dev --follow

CloudWatch Metrics
^^^^^^^^^^^^^^^^^^

Monitor Lambda metrics:

* Invocations
* Duration
* Errors
* Throttles

Set up alarms for:

* Error rate > 5%
* Duration > 80% of timeout
* Throttling events

Troubleshooting
---------------

Common Issues
^^^^^^^^^^^^^

**Deployment fails with permission error**

Ensure your AWS credentials have sufficient permissions for deployment.

**Lambda timeout**

Increase ``lambda_timeout`` in the Chalice config:

.. code-block:: json

    {
      "stages": {
        "dev": {
          "lambda_timeout": 300
        }
      }
    }

**Out of memory**

Increase ``lambda_memory_size``:

.. code-block:: json

    {
      "stages": {
        "dev": {
          "lambda_memory_size": 512
        }
      }
    }

**S3 access denied**

Verify the IAM policy includes S3 permissions for the bucket.

Rollback
^^^^^^^^

To rollback to a previous version:

1. View deployment history in AWS Lambda console
2. Select the function
3. Navigate to Versions
4. Publish a previous version as the active alias

Or redeploy from a previous git commit::

    git checkout <previous-commit>
    chalice deploy

CI/CD Integration
-----------------

GitHub Actions Example
^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: yaml

    name: Deploy

    on:
      push:
        branches: [main]

    jobs:
      deploy:
        runs-on: ubuntu-latest
        steps:
          - uses: actions/checkout@v3

          - name: Set up Python
            uses: actions/setup-python@v4
            with:
              python-version: '3.12'

          - name: Install dependencies
            run: |
              pip install -e ".[dev]"
              pip install chalice

          - name: Configure AWS credentials
            uses: aws-actions/configure-aws-credentials@v2
            with:
              aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
              aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
              aws-region: us-east-1

          - name: Deploy
            run: |
              cd amtraker_ingestion
              chalice deploy --stage prod
