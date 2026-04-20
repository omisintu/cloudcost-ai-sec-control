import os

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "cc-ai-sec-enf-287500922275-us-east-1-an")
S3_PREFIX = os.getenv("S3_PREFIX", "cur/")

DB_URL = os.getenv("DB_URL", "postgresql://postgres:SKS0987654321@localhost:5432/postgres")