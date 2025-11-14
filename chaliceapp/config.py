import boto3
from dotenv import load_dotenv
import os

load_dotenv()

AWS_PROFILE = os.environ.get("AWS_PROFILE")

if AWS_PROFILE:
    session = boto3.Session(region_name="us-east-1", profile_name=AWS_PROFILE)
else:
    session = boto3.Session(region_name="us-east-1")

s3_client = session.client("s3")

AMTRAK_ENABLED = True
VIA_ENABLED = True
BRIGHTLINE_ENABLED = False
ENVIRONMENT = "PROD"
