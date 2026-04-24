from sqlalchemy import text
from db import engine
import logging

def detect_low_value_resources():
    query = text("""
        INSERT INTO optimization_recommendations (
            resource_id,
            service,
            issue_type,
            current_cost,
            potential_savings,
            recommendation
        )
        SELECT 
            resource_id,
            product_name,
            'LOW_VALUE_USAGE' as issue_type,
            SUM(cost) as current_cost,
            SUM(cost) * 0.5 as potential_savings,
            'Resource has low usage pattern. Consider downsizing or stopping.' as recommendation
        FROM cur_data
        WHERE resource_id != 'no_resource'
        GROUP BY resource_id, product_name
        HAVING SUM(cost) < 10  -- threshold (tune later)
        ON CONFLICT DO NOTHING;
    """)

    with engine.begin() as conn:
        conn.execute(query)

    logging.info("Low-value resources detected")


def detect_data_transfer_waste():
    query = text("""
        INSERT INTO optimization_recommendations (
            resource_id,
            service,
            issue_type,
            current_cost,
            potential_savings,
            recommendation
        )
        SELECT 
            'N/A' as resource_id,
            'DataTransfer' as service,
            'HIGH_DATA_TRANSFER' as issue_type,
            SUM(cost) as current_cost,
            SUM(cost) * 0.3 as potential_savings,
            'High data transfer detected. Use CDN or optimize architecture.' as recommendation
        FROM cur_data
        WHERE product_name = 'AWSDataTransfer'
        HAVING SUM(cost) > 50
        ON CONFLICT DO NOTHING;
    """)

    with engine.begin() as conn:
        conn.execute(query)

    logging.info("Data transfer waste detected")


def detect_service_inefficiency():
    query = text("""
        INSERT INTO optimization_recommendations (
            resource_id,
            service,
            issue_type,
            current_cost,
            potential_savings,
            recommendation
        )
        SELECT 
            'N/A',
            product_name,
            'SERVICE_INEFFICIENCY',
            SUM(cost),
            SUM(cost) * 0.2,
            'Service cost is high. Evaluate usage and optimize configuration.'
        FROM cur_data
        GROUP BY product_name
        HAVING SUM(cost) > 100
        ON CONFLICT DO NOTHING;
    """)

    with engine.begin() as conn:
        conn.execute(query)

    logging.info("Service inefficiencies detected")

def run_optimization_engine():
    detect_low_value_resources()
    detect_data_transfer_waste()
    detect_service_inefficiency()