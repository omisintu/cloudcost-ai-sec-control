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
    usage_start_date TIMESTAMP
);
CREATE TABLE processed_files (
    file_key TEXT PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX cur_unique_idx ON cur_data (resource_id, usage_start_date, usage_type);

CREATE TABLE IF NOT EXISTS daily_cost (
    usage_date DATE,
    service TEXT,
    total_cost DOUBLE PRECISION,
    PRIMARY KEY (usage_date, service)
);
CREATE TABLE IF NOT EXISTS service_cost (
    service TEXT PRIMARY KEY,
    total_cost DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS resource_cost (
    resource_id TEXT,
    total_cost DOUBLE PRECISION,
    PRIMARY KEY (resource_id)
);
