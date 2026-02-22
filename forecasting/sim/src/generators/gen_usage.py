"""Generate usage_monthly table. Placeholder; returns empty DataFrame with correct schema."""

import pandas as pd


def generate_usage(config: dict) -> pd.DataFrame:
    """Generate usage monthly. For now returns empty DataFrame with seed-compatible columns."""
    return pd.DataFrame(
        columns=[
            "company_id",
            "month",
            "customer_id",
            "feature_key",
            "usage_count",
            "active_users",
        ]
    )
