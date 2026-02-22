"""Generate products table. Placeholder; returns empty DataFrame with correct schema."""

import pandas as pd


def generate_products(config: dict) -> pd.DataFrame:
    """Generate products. For now returns empty DataFrame with seed-compatible columns."""
    return pd.DataFrame(
        columns=[
            "company_id",
            "product_id",
            "product_family",
            "is_recurring",
            "default_term_months",
        ]
    )
