#------
CREATE TABLE IF NOT EXISTS cur_data (
    account_id TEXT,
    resource_id TEXT,
    usage_type TEXT,
    operation TEXT,
    usage_amount FLOAT,
    cost FLOAT,
    product_name TEXT,
    region TEXT,
    usage_start TIMESTAMP
);
CREATE TABLE processed_files (
    file_key TEXT PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX cur_unique_idx ON cur_data (resource_id, usage_start, usage_type);
