from sqlalchemy import text
from db import engine
import logging

def aggregate_daily_cost():
    query = text("""
        INSERT INTO daily_cost (usage_date, service, total_cost)
        SELECT 
            DATE(usage_start_date) as usage_date,
            product_name as service,
            SUM(cost) as total_cost
        FROM cur_data
        GROUP BY usage_date, service
        ON CONFLICT (usage_date, service)
        DO UPDATE SET total_cost = EXCLUDED.total_cost;
    """)

    with engine.begin() as conn:
        conn.execute(query)

    logging.info("Daily cost aggregation completed")


def aggregate_service_cost():
    query = text("""
        INSERT INTO service_cost (service, total_cost)
        SELECT 
            product_name as service,
            SUM(cost) as total_cost
        FROM cur_data
        GROUP BY service
        ON CONFLICT (service)
        DO UPDATE SET total_cost = EXCLUDED.total_cost;
    """)

    with engine.begin() as conn:
        conn.execute(query)

    logging.info("Service cost aggregation completed")


def aggregate_resource_cost():
    query = text("""
        INSERT INTO resource_cost (resource_id, total_cost)
        SELECT 
            resource_id,
            SUM(cost) as total_cost
        FROM cur_data
        WHERE resource_id != 'no_resource'
        GROUP BY resource_id
        ON CONFLICT (resource_id)
        DO UPDATE SET total_cost = EXCLUDED.total_cost;
    """)

    with engine.begin() as conn:
        conn.execute(query)

    logging.info("Resource cost aggregation completed")


def run_all_aggregations():
    aggregate_daily_cost()
    aggregate_service_cost()
    aggregate_resource_cost()