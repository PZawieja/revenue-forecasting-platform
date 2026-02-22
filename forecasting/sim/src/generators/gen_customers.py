"""Generate customers table. Placeholder; returns empty DataFrame with correct schema."""

import pandas as pd


def generate_customers(config: dict) -> pd.DataFrame:
    """Generate customers. For now returns empty DataFrame with seed-compatible columns."""
    return pd.DataFrame(
        columns=[
            "company_id",
            "customer_id",
            "customer_name",
            "segment",
            "region",
            "industry",
            "crm_health_input",
            "created_date",
        ]
    )
