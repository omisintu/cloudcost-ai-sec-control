import logging
from s3_client import list_cur_files, get_s3_object
from cur_processor import load_dataframe, transform_dataframe
from db import insert_batch, get_processed_files, mark_file_processed

logging.basicConfig(level=logging.INFO)

def run():
    files = list_cur_files()
    processed_files = get_processed_files()
    
    if not files:
        logging.warning("No CUR files found")
        return

    for file in files:
        key = file["key"]
        last_modified = file["last_modified"]
        
        #SKIP if already processed and unchanged
        if key in processed_files and processed_files[key] >= last_modified:
            logging.info(f"Skipping already processed file: {key}")
            continue
        
        try:
            logging.info(f"Processing new/updated file: {key}")
            obj = get_s3_object(key)
            df = load_dataframe(obj, key)
            
            if df.empty:
                logging.warning(f"No valid data before transform: {key}")
                continue
            
            df = transform_dataframe(df)
            
            if df.empty:
                logging.warning(f"No valid data after transform: {key}")
                continue
            
            insert_batch(df)
            logging.info(f"CUR Data Inserted {len(df)} rows from {key}")
            
            #Mark as processed ONLY after success
            mark_file_processed(key, last_modified)
            logging.info(f"Data Processing Completed: {key}")
            
        except Exception as e:
            logging.error(f"Failed in processing file {key}: {str(e)}")


if __name__ == "__main__":
    run()