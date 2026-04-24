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