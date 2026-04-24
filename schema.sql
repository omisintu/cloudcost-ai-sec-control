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
CREATE TABLE IF NOT EXISTS top_service_drivers (
    service TEXT PRIMARY KEY,
    total_cost DOUBLE PRECISION,
    percentage DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS top_resource_drivers (
    resource_id TEXT PRIMARY KEY,
    total_cost DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS cost_trends (
    service TEXT,
    current_cost DOUBLE PRECISION,
    previous_cost DOUBLE PRECISION,
    growth_percentage DOUBLE PRECISION,
    PRIMARY KEY (service)
);
CREATE TABLE IF NOT EXISTS optimization_recommendations (
    id SERIAL PRIMARY KEY,
    resource_id TEXT,
    service TEXT,
    issue_type TEXT,
    current_cost DOUBLE PRECISION,
    potential_savings DOUBLE PRECISION,
    recommendation TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ai_insights (
    id SERIAL PRIMARY KEY,
    insight_type TEXT,
    title TEXT,
    description TEXT,
    impact TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
