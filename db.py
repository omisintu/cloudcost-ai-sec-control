import logging
from sqlalchemy import create_engine, text
from config import DB_URL
from datetime import timezone
engine = create_engine(DB_URL)


def insert_batch(df, table="cur_data"):
    
    #df.to_csv('data.csv', index=False)# DEBUG: Save to CSV before insert to inspect data
    records = df.to_dict(orient="records")
    try:
        query = text("""
            INSERT INTO cur_data (
                account_id,
                resource_id,
                usage_type,
                operation,
                usage_amount,
                cost,
                product_name,
                region,
                usage_start_date,
                row_hash
            )
            VALUES (
                :account_id,
                :resource_id,
                :usage_type,
                :operation,
                :usage_amount,
                :cost,
                :product_name,
                :region,
                :usage_start_date,
                :row_hash
            )
            ON CONFLICT (account_id, resource_id, usage_type, operation, usage_start_date, row_hash)
            DO NOTHING
        """)

        with engine.begin() as conn:
            conn.execute(query, records)
        
    except Exception as e:
        logging.error(f"Unable to insert data: {str(e)}")
        raise
    
def get_processed_files():
    query = text("SELECT file_key, last_modified FROM processed_files")

    with engine.connect() as conn:
        result = conn.execute(query).fetchall()

    processed = {}

    for row in result:
        lm = row[1]

        #Convert DB timestamp to UTC-aware
        if lm is not None:
            lm = lm.replace(tzinfo=timezone.utc)

        processed[row[0]] = lm

    return processed


def mark_file_processed(file_key, last_modified):
    query = text("""
        INSERT INTO processed_files (file_key, last_modified)
        VALUES (:file_key, :last_modified)
        ON CONFLICT (file_key)
        DO UPDATE SET last_modified = EXCLUDED.last_modified,
                      processed_at = NOW()
    """)

    with engine.begin() as conn:
        conn.execute(query, {
            "file_key": file_key,
            "last_modified": last_modified
        })