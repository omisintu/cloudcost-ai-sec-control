import pandas as pd
import io
 

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
    print("Sample data 1:",df.sample(50))
    
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

    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

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
    print("Sample data 2:",df.sample(50))
    # Convert types safely
    #df["usage_start_date"] = pd.to_datetime(df["usage_start_date"], errors="coerce")

    #if "cost" in df.columns:
    #    df["cost"] = pd.to_numeric(df["cost"], errors="coerce").fillna(0)
    #else:
    #    df["cost"] = 0

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

    for col in final_cols:
        if col not in df.columns:
            df[col] = None

    df = df[final_cols]
    print("Sample data 3:",df.sample(50))
    return df.dropna(subset=["usage_start_date"])