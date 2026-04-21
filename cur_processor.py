import pandas as pd
import io
import sys

def load_dataframe(s3_obj, key):
    body = s3_obj["Body"].read()

    if key.endswith(".parquet"):
        df = pd.read_parquet(io.BytesIO(body))
    else:
        df = pd.read_csv(io.BytesIO(body), low_memory=False)

    return df

def transform_dataframe(df):
    import pandas as pd

    # Normalize column names
    df.columns = [c.strip() for c in df.columns]

    # DEBUG (keep this for now)
    #print("Columns:", df.columns.tolist())
    #print("Sample data 1:",df.sample(50))
    
    # Rename known fields
    rename_map = {
        "lineItem/UsageAccountId": "account_id",
        "lineItem/ResourceId": "resource_id",
        "lineItem/UsageType": "usage_type",
        "lineItem/Operation": "operation",
        "lineItem/UsageAmount": "usage_amount",
        "lineItem/UnblendedCost": "cost",
        "lineItem/ProductCode": "product_name",
        "productFrom/RegionCode": "region"
    }
    
    #df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # 🔥 CRITICAL: Detect time column (ALL POSSIBILITIES)
    if "lineItem/UsageStartDate" in df.columns:
        df["usage_start_date"] = df["lineItem/UsageStartDate"]

    elif "lineItem/UsageStartDateTime" in df.columns:
        df["usage_start_date"] = df["lineItem/UsageEndDate"]

    elif "identity/TimeInterval" in df.columns:
        df["usage_start_date"] = df["identity/TimeInterval"].astype(str).str.split("/").str[0]

    elif "bill/BillingPeriodStartDate" in df.columns:
        # 🔥 fallback (CUR 2.0 minimal export)
        df["usage_start_date"] = df["bill/BillingPeriodStartDate"]

    else:
        # 🔥 LAST fallback (prevent crash)
        print("⚠️ No time column found, using current timestamp")
        df["usage_start_date"] = pd.Timestamp.now()
    
    
    
    # Convert types safely
    df["usage_start_date"] = pd.to_datetime(df["usage_start_date"], errors="coerce")

    if "cost" in df.columns:
        df["cost"] = pd.to_numeric(df["cost"], errors="coerce").fillna(0)
    else:
        df["cost"] = 0
        
    # Drop everything that is NOT in your list
    df.drop(df.columns.difference(['line_item_usage_account_id', 'line_item_resource_id', 'line_item_usage_type', 'line_item_operation', 'line_item_usage_amount', 'line_item_unblended_cost', 'line_item_product_code', 'product_from_region_code', 'usage_start_date']), axis=1, inplace=True)
    
    # Format: {'old_name': 'new_name'}
    df = df.rename(columns={'line_item_usage_account_id': 'account_id', 'line_item_resource_id': 'resource_id', 'line_item_usage_type': 'usage_type', 'line_item_operation': 'operation', 'line_item_usage_amount': 'usage_amount', 'line_item_unblended_cost': 'cost', 'line_item_product_code': 'product_name', 'product_from_region_code': 'region', 'usage_start_date': 'usage_start_date'})

    # Reorder by selecting columns in a specific list
    df = df[['account_id', 'resource_id', 'usage_type', 'operation', 'usage_amount', 'cost', 'product_name', 'region', 'usage_start_date']]
    
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
    #print("Columns A:", df.columns.tolist())
    #for col in final_cols:
    #    if col not in df.columns:
    #        df[col] = None
    #print("Sample data 2:",df.sample(50))
    #df = df[final_cols]
    return df