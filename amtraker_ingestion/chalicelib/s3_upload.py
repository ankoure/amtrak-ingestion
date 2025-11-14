from datetime import date, datetime, timedelta
import glob
from chalicelib.config import s3_client
from io import BytesIO
import gzip
import os
import time
from chalicelib.constants import S3_BUCKET, S3_DATA_TEMPLATE, EASTERN_TIME, LOCAL_DATA_TEMPLATE
from chalicelib.disk import DATA_DIR
import json


def get_s3_json(bucket_name: str, key: str) -> dict:
    """
    Downloads a JSON file from S3 and returns it as a dict.
    """
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    content = response["Body"].read().decode("utf-8")
    return json.loads(content)


def set_s3_json(data: dict, bucket_name: str, key: str):
    """
    Uploads a Python dict as a JSON file to S3.
    """

    json_bytes = json.dumps(data, indent=2).encode("utf-8")

    s3_client.put_object(
        Bucket=bucket_name, Key=key, Body=json_bytes, ContentType="application/json"
    )


def service_date(ts: datetime) -> date:
    # In practice a None TZ is UTC, but we want to be explicit
    # In many places we have an implied eastern
    ts = ts.replace(tzinfo=EASTERN_TIME)

    if ts.hour >= 3 and ts.hour <= 23:
        return date(ts.year, ts.month, ts.day)

    prior = ts - timedelta(days=1)
    return date(prior.year, prior.month, prior.day)


def _compress_and_upload_file(fp: str):
    """Compress a file in-memory and upload to S3."""
    # generate output location
    # Handle both /tmp (Lambda) and data/ (local) paths
    if fp.startswith("/tmp/"):
        # In Lambda: /tmp/raw/Provider/... -> raw/Provider/...
        rp = fp.replace("/tmp/", "")
    else:
        # Local: data/raw/Provider/... -> raw/Provider/...
        rp = os.path.relpath(fp, DATA_DIR)
    s3_key = S3_DATA_TEMPLATE.format(relative_path=rp)

    with open(fp, "rb") as f:
        # gzip to buffer and upload
        gz_bytes = gzip.compress(f.read())
        buffer = BytesIO(gz_bytes)

        # Determine content type from file extension
        content_type = "application/json" if fp.endswith(".json") else "text/csv"
        s3_client.upload_fileobj(
            buffer,
            S3_BUCKET,
            Key=s3_key,
            ExtraArgs={"ContentType": content_type, "ContentEncoding": "gzip"},
        )


def upload_todays_events_to_s3():
    """Upload today's events to the TM s3 bucket."""
    start_time = time.time()

    pull_date = service_date(datetime.now(EASTERN_TIME))

    # get files updated for this service date
    # TODO: only update modified files? cant imagine much of a difference if we partition live data by day
    files_updated_today = glob.glob(
        LOCAL_DATA_TEMPLATE.format(
            year=pull_date.year, month=pull_date.month, day=pull_date.day
        )
    )

    # upload them to s3, gzipped
    for fp in files_updated_today:
        _compress_and_upload_file(fp)

    end_time = time.time()
    print(end_time - start_time)


if __name__ == "__main__":
    upload_todays_events_to_s3()
