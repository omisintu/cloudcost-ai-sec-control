import pandas as pd
import io
import sys
import hashlib
import logging

def generate_row_hash(row):
    key = f"{row.get('account_id','')}-{row.get('resource_id','')}-{row.get('usage_type','')}-{row.get('operation','')}-{row.get('usage_start','')}-{row.get('cost','')}"
    return hashlib.sha256(key.encode()).hexdigest()

def load_dataframe(s3_obj, key):
    body = s3_obj["Body"].read()

    if key.endswith(".parquet"):
        df = pd.read_parquet(io.BytesIO(body))
    else:
        df = pd.read_csv(io.BytesIO(body), low_memory=False)

    return df

def transform_dataframe(df):

    # Normalize column names
    df.columns = [c.strip() for c in df.columns]

    # Drop everything that is NOT in your list
    df.drop(df.columns.difference(['line_item_usage_account_id', 'line_item_resource_id', 'line_item_usage_type', 'line_item_operation', 'line_item_usage_amount', 'line_item_unblended_cost', 'line_item_product_code', 'product_from_region_code', 'line_item_usage_start_date','identity_line_item_id']), axis=1, inplace=True)
    
    # Format: {'old_name': 'new_name'}
    df = df.rename(columns={'line_item_usage_account_id': 'account_id', 'line_item_resource_id': 'resource_id', 'line_item_usage_type': 'usage_type', 'line_item_operation': 'operation', 'line_item_usage_amount': 'usage_amount', 'line_item_unblended_cost': 'cost', 'line_item_product_code': 'product_name', 'product_from_region_code': 'region', 'line_item_usage_start_date': 'usage_start_date','identity_line_item_id':'row_hash'})

    # Reorder by selecting columns in a specific list
    df = df[['account_id', 'resource_id', 'usage_type', 'operation', 'usage_amount', 'cost', 'product_name', 'region', 'usage_start_date','row_hash']]
    
    #print("Sample data 4:",df.sample(50))
    #print("Columns B:", df.columns.tolist())
    #sys.exit("Execution Terminated.......") # Exits with status 1

    # Ensure required columns exist
    final_cols = [
        "account_id",
        "resource_id",
        "usage_type",
        "operation",
        "usage_amount",
        "cost",
        "product_name",
        "region",
        "usage_start_date"
    ]
    
    # FIX: Replace nulls with stable placeholders
    df["resource_id"] = df["resource_id"].fillna("no_resource")
    df["operation"] = df["operation"].fillna("no_operation")

    df["usage_start_date"] = pd.to_datetime(df["usage_start_date"], errors="coerce")
    df["cost"] = pd.to_numeric(df["cost"], errors="coerce").fillna(0)
    #df = df.dropna(subset=["usage_start_date"])

    # Generate hash
    df["row_hash"] = df.apply(generate_row_hash, axis=1)
    logging.info(f"Columns in file: {list(df.columns)}")
    
    return df
    #return df.dropna(subset=["usage_start_date"])