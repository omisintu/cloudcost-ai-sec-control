import logging
from sqlalchemy import create_engine
from config import DB_URL

engine = create_engine(DB_URL, pool_size=10, max_overflow=20)


def insert_batch(df, table="cur_data"):
    if "usage_start_date" not in df.columns:
        raise Exception("usage_start_date missing before DB insert")

    try:
        df.to_sql(
            table,
            engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000
        )
    except Exception as e:
        logging.error(f"Unable to insert data: {str(e)}")
        raise