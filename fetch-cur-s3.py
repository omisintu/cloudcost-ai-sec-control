import boto3

s3 = boto3.client('s3')

BUCKET = "cc-ai-sec-enf-287500922275-us-east-1-an"
PREFIX = "cur/"

def list_cur_files():
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
    return [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]

#Download CUR data
import pandas as pd
import io

def load_parquet_from_s3(key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_parquet(io.BytesIO(obj['Body'].read()))


#Extract Cols
REQUIRED_COLUMNS = [
    "lineItem/UsageAccountId",
    "lineItem/ResourceId",
    "lineItem/UsageType",
    "lineItem/Operation",
    "lineItem/UsageAmount",
    "lineItem/UnblendedCost",
    "product/ProductName",
    "product/region",
    "lineItem/UsageStartDate"
]

def filter_columns(df):
    return df[[col for col in REQUIRED_COLUMNS if col in df.columns]]

#Insert data to DB
from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres:SKS0987654321@aws-cc-ai.c452oegagckx.us-east-1.rds.amazonaws.com:5432/postgres")

def save_to_db(df):
    df = df.rename(columns={
        "lineItem/UsageAccountId": "usage_account_id",
        "lineItem/ResourceId": "resource_id",
        "lineItem/UsageType": "usage_type",
        "lineItem/Operation": "operation",
        "lineItem/UsageAmount": "usage_amount",
        "lineItem/UnblendedCost": "cost",
        "product/ProductName": "product_name",
        "product/region": "region",
        "lineItem/UsageStartDate": "usage_start"
    })

    df.to_sql("cur_data", engine, if_exists="append", index=False)
    
