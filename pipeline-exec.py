import fetchdata

def run_ingestion():
    files = list_cur_files()
    
    for file in files:
        df = load_parquet_from_s3(file)
        df = filter_columns(df)
        save_to_db(df)

if __name__ == "__main__":
    run_ingestion()