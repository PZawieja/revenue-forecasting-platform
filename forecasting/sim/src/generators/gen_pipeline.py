"""Generate pipeline_opportunities_snapshot table. Placeholder; returns empty DataFrame with correct schema."""

import pandas as pd


def generate_pipeline(config: dict) -> pd.DataFrame:
    """Generate pipeline snapshots. For now returns empty DataFrame with seed-compatible columns."""
    return pd.DataFrame(
        columns=[
            "company_id",
            "snapshot_date",
            "opportunity_id",
            "customer_id",
            "segment",
            "stage",
            "amount",
            "expected_close_date",
            "opportunity_type",
        ]
    )
