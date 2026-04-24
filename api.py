from fastapi import FastAPI
from sqlalchemy import text
from db import engine

app = FastAPI()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/cost/service")
def get_service_cost():
    query = text("SELECT * FROM service_cost ORDER BY total_cost DESC LIMIT 10")

    with engine.connect() as conn:
        result = conn.execute(query).fetchall()

    return [dict(row._mapping) for row in result]


@app.get("/cost/daily")
def get_daily_cost():
    query = text("""
        SELECT * FROM daily_cost 
        ORDER BY usage_date DESC 
        LIMIT 30
    """)

    with engine.connect() as conn:
        result = conn.execute(query).fetchall()

    return [dict(row._mapping) for row in result]


@app.get("/cost/top-resources")
def get_top_resources():
    query = text("""
        SELECT * FROM resource_cost
        ORDER BY total_cost DESC
        LIMIT 10
    """)

    with engine.connect() as conn:
        result = conn.execute(query).fetchall()

    return [dict(row._mapping) for row in result]

@app.get("/drivers/services")
def top_services():
    query = text("""
        SELECT * FROM top_service_drivers
        ORDER BY total_cost DESC
        LIMIT 10
    """)

    with engine.connect() as conn:
        result = conn.execute(query).fetchall()

    return [dict(row._mapping) for row in result]

@app.get("/drivers/resources")
def top_resources():
    query = text("""
        SELECT * FROM top_resource_drivers
        ORDER BY total_cost DESC
        LIMIT 10
    """)

    with engine.connect() as conn:
        result = conn.execute(query).fetchall()

    return [dict(row._mapping) for row in result]

@app.get("/drivers/trends")
def cost_trends():
    query = text("""
        SELECT * FROM cost_trends
        WHERE growth_percentage > 20
        ORDER BY growth_percentage DESC
        LIMIT 10
    """)

    with engine.connect() as conn:
        result = conn.execute(query).fetchall()

    return [dict(row._mapping) for row in result]
