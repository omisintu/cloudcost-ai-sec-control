import boto3
from botocore.config import Config
from tenacity import retry, stop_after_attempt, wait_exponential
from config import AWS_REGION, S3_BUCKET, S3_PREFIX

boto_config = Config(
    retries={"max_attempts": 10, "mode": "standard"}
)

s3 = boto3.client("s3", region_name=AWS_REGION, config=boto_config)


def list_cur_files():
    paginator = s3.get_paginator("list_objects_v2")

    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet") or key.endswith(".csv"):
                files.append({
                    "key": key,
                    "last_modified": obj["LastModified"]
                })

    return files

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=2))
def get_s3_object(key):
    return s3.get_object(Bucket=S3_BUCKET, Key=key)