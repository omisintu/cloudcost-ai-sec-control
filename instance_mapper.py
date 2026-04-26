def recommend_ec2_instance(current_type):
    """
    Simple downsize logic (safe MVP version)
    """

    downsizing_map = {
        "t3.large": "t3.medium",
        "t3.medium": "t3.small",
        "t3.small": "t3.micro",
        "t2.large": "t2.medium",
        "t2.medium": "t2.small",
        "t2.small": "t2.micro",
    }

    return downsizing_map.get(current_type, current_type)

def recommend_rds_instance(current_type):
    downsizing_map = {
        "db.t3.large": "db.t3.medium",
        "db.t3.medium": "db.t3.small",
        "db.t3.small": "db.t3.micro",
        "db.t4g.large": "db.t4g.medium",
        "db.t4g.medium": "db.t4g.small",
    }

    return downsizing_map.get(current_type, current_type)
