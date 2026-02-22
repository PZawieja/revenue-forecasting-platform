"""Generate subscription_line_items: contracts over time with renewal/churn and expansion/contraction."""

from __future__ import annotations

import pandas as pd
import numpy as np
from typing import Any

COMPANY_ID = 1
SEGMENTS = ["enterprise", "large", "medium", "smb"]


def _segment_behavior(config: dict, segment: str) -> dict:
    el = config.get("enterprise_large_behavior", {})
    ms = config.get("mid_smb_behavior", {})
    if segment in ("enterprise", "large"):
        return {"term_months": el.get("contract_term_months", [12, 24]), "onboarding_lag": el.get("onboarding_lag_months", [1, 2, 3])}
    return {"term_months": ms.get("contract_term_months", [1, 12]), "onboarding_lag": ms.get("onboarding_lag_months", [0, 1])}


def _annual_to_per_renewal(annual_churn: float, term_months: int) -> float:
    if term_months <= 0:
        return 0
    periods_per_year = 12 / term_months
    return 1 - (1 - annual_churn) ** (1 / periods_per_year)


def generate_subscriptions(
    config: dict,
    calendar_dates: list,
    products_df: pd.DataFrame,
    customers_df: pd.DataFrame,
    latents: dict[str, Any],
    company_id: int = 1,
    rng: np.random.Generator | None = None,
) -> pd.DataFrame:
    if rng is None:
        rng = np.random.default_rng(config.get("random_seed", 42))

    churn_targets = config.get("churn_targets_by_segment", {})
    months = len(calendar_dates)
    recurring = products_df[products_df["is_recurring"]].copy()
    if recurring.empty:
        recurring = products_df.head(4)
    product_by_family = recurring.groupby("product_family").first().reset_index()
    family_to_pid = dict(zip(product_by_family["product_family"].str.lower(), product_by_family["product_id"]))
    all_families = list(family_to_pid.keys())

    rows = []
    contract_counter = [0]

    def next_contract_id():
        contract_counter[0] += 1
        return f"c{contract_counter[0]}"

    n_customers = len(customers_df)
    latent_health = latents["latent_health"]
    expansion_propensity = latents["expansion_propensity"]
    price_sensitivity = latents["price_sensitivity"]
    created_month_index = latents["created_month_index"]
    segments = customers_df["segment"].values

    # Segment-based quantity/price ranges (long-tail: use power to skew)
    def qty_price(seg: str, rng: np.random.Generator):
        u = rng.random()
        if seg == "enterprise":
            qty = int(50 + 450 * (u ** 0.4))
            price = 200 + 1800 * (u ** 0.5)
        elif seg == "large":
            qty = int(20 + 180 * (u ** 0.45))
            price = 100 + 700 * (u ** 0.5)
        elif seg == "medium":
            qty = int(5 + 45 * (u ** 0.5))
            price = 30 + 170 * (u ** 0.5)
        else:
            qty = int(1 + 19 * (u ** 0.6))
            price = 10 + 70 * (u ** 0.5)
        return max(1, qty), max(1.0, price)

    def discount(seg: str, rng: np.random.Generator):
        if seg in ("enterprise", "large"):
            return round(rng.uniform(0.05, 0.25), 3)
        if seg == "medium":
            return round(rng.uniform(0, 0.12), 3)
        return round(rng.uniform(0, 0.05), 3)

    for i in range(n_customers):
        seg = segments[i]
        beh = _segment_behavior(config, seg)
        n_families = rng.integers(1, 4) if seg in ("enterprise", "large") else (rng.integers(1, 3) if seg == "medium" else 1)
        chosen = rng.choice(len(all_families), size=min(n_families, len(all_families)), replace=False)
        families = [all_families[j] for j in chosen]
        product_ids = [family_to_pid[f] for f in families if f in family_to_pid]
        if not product_ids:
            product_ids = [recurring["product_id"].iloc[0]]

        start_idx = int(created_month_index[i]) + int(rng.choice(beh["onboarding_lag"]))
        start_idx = max(0, min(start_idx, months - 1))
        term_months = int(rng.choice(beh["term_months"]))
        annual = churn_targets.get(seg, 0.10)
        base_churn = _annual_to_per_renewal(annual, term_months)

        qty, price = qty_price(seg, rng)
        disc = discount(seg, rng)
        billing = "annual" if seg in ("enterprise", "large") and rng.random() > 0.2 else rng.choice(["monthly", "annual"])

        while start_idx < months:
            end_idx = start_idx + term_months
            start_date = calendar_dates[start_idx]
            end_month_idx = min(end_idx, months) - 1
            end_date = calendar_dates[end_month_idx]
            end_date_str = end_date if isinstance(end_date, str) else pd.Timestamp(end_date).strftime("%Y-%m-%d")
            start_date_str = start_date if isinstance(start_date, str) else pd.Timestamp(start_date).strftime("%Y-%m-%d")

            cid = next_contract_id()
            for pid in product_ids:
                rows.append({
                    "company_id": company_id,
                    "contract_id": cid,
                    "customer_id": i + 1,
                    "product_id": pid,
                    "contract_start_date": start_date_str,
                    "contract_end_date": end_date_str,
                    "billing_frequency": billing,
                    "quantity": qty,
                    "unit_price": price,
                    "discount_pct": disc,
                    "status": "active",
                })

            if end_idx > months:
                break
            # Renewal roll
            h = latent_health[i]
            churn_prob = base_churn * (1.2 - h) + rng.uniform(-0.05, 0.08)
            churn_prob = np.clip(churn_prob, 0.02, 0.95)
            if rng.random() < churn_prob:
                # Churn: set status cancelled for this contract
                for r in rows[-len(product_ids):]:
                    r["status"] = "cancelled"
                break
            # Renew: next contract, possible expansion/contraction
            start_idx = end_idx
            if rng.random() < 0.35 * expansion_propensity[i]:
                qty = min(int(qty * rng.uniform(1.05, 1.35)), 2000)
            elif rng.random() < 0.2:
                qty = max(1, int(qty * rng.uniform(0.85, 1.0)))
            if rng.random() < 0.25 * price_sensitivity[i]:
                price = round(price * rng.uniform(0.95, 1.0), 2)
            disc = discount(seg, rng)

    df = pd.DataFrame(rows)
    return df
