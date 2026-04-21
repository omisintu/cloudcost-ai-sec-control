import logging
from s3_client import list_cur_files, get_s3_object
from cur_processor import load_dataframe, transform_dataframe
from db import insert_batch

logging.basicConfig(level=logging.INFO)


def process_file(key):
    logging.info(f"Processing {key}")

    obj = get_s3_object(key)
    df = load_dataframe(obj, key)

    if df.empty:
        logging.warning(f"Empty file: {key}")
        return
    
    print(df.columns.tolist())
    df = transform_dataframe(df)
    
    if df.empty:
        logging.warning(f"No valid data after transform: {key}")
        return

    insert_batch(df)
    logging.info(f"Inserted {len(df)} rows from {key}")


def run():
    files = list_cur_files()

    if not files:
        logging.warning("No CUR files found")
        return

    for key in files:
        try:
            process_file(key)
        except Exception as e:
            logging.error(f"Failed in processing file {key}: {str(e)}")


if __name__ == "__main__":
    run()