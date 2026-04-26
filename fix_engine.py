from sqlalchemy import text
from db import engine
from instance_mapper import recommend_ec2_instance, recommend_rds_instance

def generate_ec2_stop_actions():
    query = text("""
        SELECT resource_id
        FROM optimization_recommendations
        WHERE recommendation = 'STOP_INSTANCE'
    """)

    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    actions = []

    for row in rows:
        instance_id = row.resource_id.split("/")[-1] if row.resource_id else "UNKNOWN"

        actions.append({
            "service": "EC2",
            "resource_id": instance_id,
            "action_type": "STOP_INSTANCE",
            "cli_command": f"aws ec2 stop-instances --instance-ids {instance_id}",
            "terraform_code": f"""
resource "aws_instance" "{instance_id}" {{
  instance_type = "t3.micro" # adjust as needed
  lifecycle {{
    prevent_destroy = false
  }}
}}
""",
            "risk_level": "MEDIUM"
        })

    return actions

def generate_eip_release_actions():
    query = text("""
        SELECT resource_id
        FROM optimization_recommendations
        WHERE recommendation = 'RELEASE_EIP'
    """)

    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    actions = []

    for row in rows:
        allocation_id = row.resource_id

        actions.append({
            "service": "EC2",
            "resource_id": allocation_id,
            "action_type": "RELEASE_EIP",
            "cli_command": f"aws ec2 release-address --allocation-id {allocation_id}",
            "terraform_code": f"# Remove unused EIP: {allocation_id}",
            "risk_level": "LOW"
        })

    return actions


def store_actions(actions):
    insert_query = text("""
        INSERT INTO auto_fix_actions (
            service,
            resource_id,
            action_type,
            cli_command,
            terraform_code,
            risk_level
        )
        VALUES (
            :service,
            :resource_id,
            :action_type,
            :cli_command,
            :terraform_code,
            :risk_level
        )
    """)

    with engine.begin() as conn:
        conn.execute(insert_query, actions)

def generate_ec2_resize_actions():
    query = text("""
        SELECT resource_id, current_instance_type
        FROM optimization_recommendations
        WHERE recommendation = 'DOWNSIZE_EC2'
    """)

    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    actions = []

    for row in rows:
        instance_id = row.resource_id.split("/")[-1]
        current_type = row.current_instance_type

        recommended_type = recommend_ec2_instance(current_type)

        if current_type == recommended_type:
            continue  # skip if no change

        actions.append({
            "service": "EC2",
            "resource_id": instance_id,
            "action_type": "RESIZE_INSTANCE",
            "cli_command": f"""
aws ec2 modify-instance-attribute \
--instance-id {instance_id} \
--instance-type \"{{\\\"Value\\\": \\\"{recommended_type}\\\"}}\"
""",
            "terraform_code": f"""
resource "aws_instance" "{instance_id}" {{
  instance_type = "{recommended_type}"
}}
""",
            "risk_level": "HIGH"
        })

    return actions


def generate_rds_resize_actions():
    query = text("""
        SELECT resource_id, current_instance_type
        FROM optimization_recommendations
        WHERE recommendation = 'DOWNSIZE_RDS'
    """)

    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    actions = []

    for row in rows:
        db_id = row.resource_id.split(":")[-1]
        current_type = row.current_instance_type

        recommended_type = recommend_rds_instance(current_type)

        if current_type == recommended_type:
            continue

        actions.append({
            "service": "RDS",
            "resource_id": db_id,
            "action_type": "RESIZE_RDS",
            "cli_command": f"""
aws rds modify-db-instance \
--db-instance-identifier {db_id} \
--db-instance-class {recommended_type} \
--apply-immediately
""",
            "terraform_code": f"""
resource "aws_db_instance" "{db_id}" {{
  instance_class = "{recommended_type}"
}}
""",
            "risk_level": "HIGH"
        })

    return actions



def run_fix_engine():
    actions = []

    actions += generate_ec2_stop_actions()
    actions += generate_ec2_resize_actions()
    actions += generate_rds_resize_actions()
    actions += generate_eip_release_actions()

    if actions:
        store_actions(actions)