from sqlalchemy import text
from db import engine
import logging
from ai_utils import enhance_with_ai
def generate_cost_spike_insights():
    query = text("""
        SELECT service, growth_percentage, current_cost
        FROM cost_trends
        WHERE growth_percentage > 20
        ORDER BY growth_percentage DESC
        LIMIT 5
    """)

    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    insights = []

    for row in rows:
        service = row.service
        growth = round(row.growth_percentage, 2)
        cost = round(row.current_cost, 2)

        insights.append({
            "type": "COST_SPIKE",
            "title": f"{service} cost increased by {growth}%",
            "description": f"{service} usage increased significantly in recent days.",
            "impact": f"Current cost is ₹{cost}. Investigate usage spike."
        })

    return insights

def generate_savings_insights():
    query = text("""
        SELECT service, SUM(potential_savings) as savings
        FROM optimization_recommendations
        GROUP BY service
        ORDER BY savings DESC
        LIMIT 5
    """)

    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    insights = []

    for row in rows:
        service = row.service
        savings = round(row.savings, 2)

        insights.append({
            "type": "SAVINGS",
            "title": f"Potential savings in {service}",
            "description": f"Optimization opportunities detected in {service}.",
            "impact": f"You can save approximately ₹{savings}/month."
        })

    return insights

def generate_cost_driver_insights():
    query = text("""
        SELECT service, percentage
        FROM top_service_drivers
        ORDER BY percentage DESC
        LIMIT 3
    """)

    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    insights = []

    for row in rows:
        service = row.service
        pct = round(row.percentage, 2)

        insights.append({
            "type": "COST_DRIVER",
            "title": f"{service} is a major cost driver",
            "description": f"{service} contributes {pct}% of total cost.",
            "impact": "Focus optimization efforts here for maximum savings."
        })

    return insights

def store_insights(insights):
    insert_query = text("""
        INSERT INTO ai_insights (insight_type, title, description, impact)
        VALUES (:type, :title, :description, :impact)
    """)

    with engine.begin() as conn:
        conn.execute(insert_query, insights)

def run_ai_insights():
    all_insights = []

    all_insights += generate_cost_spike_insights()
    all_insights += generate_savings_insights()
    all_insights += generate_cost_driver_insights()

    enhanced_insights = []

    for insight in all_insights:
        polished = enhance_with_ai(
            insight["title"],
            insight["description"],
            insight["impact"]
        )

        enhanced_insights.append({
            "type": insight["type"],
            "title": insight["title"],
            "description": polished,
            "impact": insight["impact"]
        })

    if enhanced_insights:
        store_insights(enhanced_insights)
        logging.info("AI insights generated")


#def run_ai_insights():
#    all_insights = []

#    all_insights += generate_cost_spike_insights()
#    all_insights += generate_savings_insights()
#    all_insights += generate_cost_driver_insights()

#    if all_insights:
        #store_insights(all_insights)

#    logging.info("AI insights generated")