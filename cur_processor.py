import pandas as pd
import io

REQUIRED_COLUMNS = [
    "lineItem/AccountId",
    "lineItem/ResourceId",
    "lineItem/UsageType",
    "lineItem/Operation",
    "lineItem/UsageAmount",
    "lineItem/UnblendedCost",
    "product/ProductName",
    "product/region",
    "lineItem/UsageStartDate"
]


def load_dataframe(s3_obj, key):
    body = s3_obj["Body"].read()

    if key.endswith(".parquet"):
        df = pd.read_parquet(io.BytesIO(body))
    else:
        df = pd.read_csv(io.BytesIO(body), low_memory=False)

    return df


def transform_dataframe(df):
    cols = [c for c in REQUIRED_COLUMNS if c in df.columns]
    df = df[cols].copy()

    df = df.rename(columns={
        "lineItem/AccountId": "account_id",
        "lineItem/ResourceId": "resource_id",
        "lineItem/UsageType": "usage_type",
        "lineItem/Operation": "operation",
        "lineItem/UsageAmount": "usage_amount",
        "lineItem/UnblendedCost": "cost",
        "product/ProductName": "product_name",
        "product/region": "region",
        "lineItem/UsageStartDate": "usage_start"
    })

    df["usage_start"] = pd.to_datetime(df["usage_start"], errors="coerce")
    df["cost"] = pd.to_numeric(df["cost"], errors="coerce").fillna(0)

    return df.dropna(subset=["usage_start"])