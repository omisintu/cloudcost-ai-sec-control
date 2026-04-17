import fetchdata

def run_ingestion():
    files = fetchdata.list_cur_files()
    
    for file in files:
        df = fetchdata.load_parquet_from_s3(file)
        df = fetchdata.filter_columns(df)
        fetchdata.save_to_db(df)

if __name__ == "__main__":
    run_ingestion()