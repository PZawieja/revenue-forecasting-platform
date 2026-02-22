"""Generate subscription_line_items table. Placeholder; returns empty DataFrame with correct schema."""

import pandas as pd


def generate_subscriptions(config: dict) -> pd.DataFrame:
    """Generate subscription line items. For now returns empty DataFrame with seed-compatible columns."""
    return pd.DataFrame(
        columns=[
            "company_id",
            "contract_id",
            "customer_id",
            "product_id",
            "contract_start_date",
            "contract_end_date",
            "billing_frequency",
            "quantity",
            "unit_price",
            "discount_pct",
            "status",
        ]
    )
