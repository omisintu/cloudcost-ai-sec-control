from sqlalchemy import text
from db import engine
import logging

def compute_top_service_drivers():
    query = text("""
        WITH total AS (
            SELECT SUM(cost) as total_cost FROM cur_data
        )
        INSERT INTO top_service_drivers (service, total_cost, percentage)
        SELECT 
            product_name as service,
            SUM(cost) as total_cost,
            (SUM(cost) / total.total_cost) * 100 as percentage
        FROM cur_data, total
        GROUP BY service, total.total_cost
        ON CONFLICT (service)
        DO UPDATE SET 
            total_cost = EXCLUDED.total_cost,
            percentage = EXCLUDED.percentage;
    """)

    with engine.begin() as conn:
        conn.execute(query)

    logging.info("Top service drivers computed")

def compute_top_resource_drivers():
    query = text("""
        INSERT INTO top_resource_drivers (resource_id, total_cost)
        SELECT 
            resource_id,
            SUM(cost) as total_cost
        FROM cur_data
        WHERE resource_id != 'no_resource'
        GROUP BY resource_id
        ORDER BY total_cost DESC
        LIMIT 50
        ON CONFLICT (resource_id)
        DO UPDATE SET total_cost = EXCLUDED.total_cost;
    """)

    with engine.begin() as conn:
        conn.execute(query)

    logging.info("Top resource drivers computed")

def compute_cost_trends():
    query = text("""
        WITH daily AS (
            SELECT 
                product_name as service,
                DATE(usage_start_date) as dt,
                SUM(cost) as daily_cost
            FROM cur_data
            GROUP BY service, dt
        ),
        latest AS (
            SELECT service, SUM(daily_cost) as current_cost
            FROM daily
            WHERE dt >= CURRENT_DATE - INTERVAL '3 days'
            GROUP BY service
        ),
        previous AS (
            SELECT service, SUM(daily_cost) as previous_cost
            FROM daily
            WHERE dt BETWEEN CURRENT_DATE - INTERVAL '6 days'
                        AND CURRENT_DATE - INTERVAL '4 days'
            GROUP BY service
        )
        INSERT INTO cost_trends (service, current_cost, previous_cost, growth_percentage)
        SELECT 
            l.service,
            l.current_cost,
            COALESCE(p.previous_cost, 0),
            CASE 
                WHEN COALESCE(p.previous_cost, 0) = 0 THEN 100
                ELSE ((l.current_cost - p.previous_cost) / p.previous_cost) * 100
            END as growth_percentage
        FROM latest l
        LEFT JOIN previous p ON l.service = p.service
        ON CONFLICT (service)
        DO UPDATE SET
            current_cost = EXCLUDED.current_cost,
            previous_cost = EXCLUDED.previous_cost,
            growth_percentage = EXCLUDED.growth_percentage;
    """)

    with engine.begin() as conn:
        conn.execute(query)

    logging.info("Cost trends computed")


def run_driver_engine():
    compute_top_service_drivers()
    compute_top_resource_drivers()
    compute_cost_trends()