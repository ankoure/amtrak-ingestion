import boto3
import os

# Only load dotenv if not running in Lambda
if "AWS_EXECUTION_ENV" not in os.environ:
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

AWS_PROFILE = os.environ.get("AWS_PROFILE")

# In Lambda, don't use profiles - use IAM role
if AWS_PROFILE and "AWS_EXECUTION_ENV" not in os.environ:
    session = boto3.Session(region_name="us-east-1", profile_name=AWS_PROFILE)
else:
    session = boto3.Session(region_name="us-east-1")

s3_client = session.client("s3")

AMTRAK_ENABLED = True
VIA_ENABLED = True
BRIGHTLINE_ENABLED = False
ENVIRONMENT = "PROD"
