"""Generate products table: 4 families A/B/C/D, recurring + optional add-on."""

import pandas as pd


def generate_products(config: dict, company_id: int = 1, rng=None) -> pd.DataFrame:
    if rng is None:
        import numpy as np
        rng = np.random.default_rng(config.get("random_seed", 42))

    rows = []
    pid = 1
    # 4 families: A, B, C, D. Two products per family: main (recurring) + optional add-on
    for family in ["a", "b", "c", "d"]:
        # Main recurring product
        term = int(rng.choice([12, 24]))
        rows.append({
            "company_id": company_id,
            "product_id": f"p{pid}",
            "product_family": family,
            "is_recurring": True,
            "default_term_months": term,
        })
        pid += 1
        # Add-on: one non-recurring for family d
        if family == "d":
            rows.append({
                "company_id": company_id,
                "product_id": f"p{pid}",
                "product_family": family,
                "is_recurring": False,
                "default_term_months": 12,
            })
            pid += 1

    return pd.DataFrame(rows)
