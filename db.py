import logging
from sqlalchemy import create_engine, text
from config import DB_URL

engine = create_engine(DB_URL)


def insert_batch(df, table="cur_data"):
    #if "usage_start_date" not in df.columns:
    #    raise Exception("usage_start_date missing before DB insert")
    
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
                usage_start_date
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
                :usage_start_date
            )
            ON CONFLICT (resource_id, usage_start_date, usage_type, operation)
            DO NOTHING
        """)

        with engine.begin() as conn:
            conn.execute(query, records)
        

        #df.to_sql(
        #    table,
        #    engine,
        #    if_exists="append",
        #    index=True,
        #    method="multi",
        #    chunksize=7000
        #)
    except Exception as e:
        logging.error(f"Unable to insert data: {str(e)}")
        raise
def get_processed_files():
    query = text("SELECT file_key, last_modified FROM processed_files")
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()

    return {row[0]: row[1] for row in result}


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