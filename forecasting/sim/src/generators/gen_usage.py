"""Generate usage_monthly: customer x month x feature, influenced by health, onboarding, seasonality, noise."""

from __future__ import annotations

import pandas as pd
import numpy as np
from typing import Any


def generate_usage(
    config: dict,
    calendar_dates: list,
    customers_df: pd.DataFrame,
    subscriptions_df: pd.DataFrame,
    latents: dict[str, Any],
    company_id: int = 1,
    rng: np.random.Generator | None = None,
) -> pd.DataFrame:
    if rng is None:
        rng = np.random.default_rng(config.get("random_seed", 42))

    features = config.get("usage", {}).get("features", ["feature_a", "feature_b", "feature_c"])
    noise_std = config.get("usage", {}).get("noise_std", 0.25)
    months = len(calendar_dates)
    n_customers = len(customers_df)
    latent_health = latents["latent_health"]
    created_month_index = latents["created_month_index"]

    # Build (customer_id, month_index) active: any contract covering that month
    customer_months = set()
    churn_month_by_customer = {}
    base_ts = pd.Timestamp(calendar_dates[0]) if calendar_dates else pd.Timestamp("2024-01-01")
    for _, row in subscriptions_df.iterrows():
        try:
            start = pd.Timestamp(row["contract_start_date"])
            end = pd.Timestamp(row["contract_end_date"])
        except Exception:
            continue
        cid = row["customer_id"]
        for m in range(months):
            cal = calendar_dates[m]
            cal_ts = pd.Timestamp(cal) if not isinstance(cal, str) else pd.Timestamp(cal)
            if start <= cal_ts <= end:
                customer_months.add((cid, m))
        if row["status"] == "cancelled":
            end_idx = min(months - 1, max(0, (end - base_ts).days // 30))
            churn_month_by_customer[cid] = max(churn_month_by_customer.get(cid, -1), end_idx)

    rows = []
    for (cid, m) in customer_months:
        idx = cid - 1
        h = latent_health[idx] if idx < len(latent_health) else 0.7
        created_idx = int(created_month_index[idx]) if idx < len(created_month_index) else 0
        months_since_start = m - created_idx
        onboarding_factor = 1.0 if months_since_start >= 3 else (0.4 + 0.2 * months_since_start)
        seasonality = 1.0 + 0.1 * np.sin(2 * np.pi * m / 12)
        churn_m = churn_month_by_customer.get(cid)
        decline = 1.0
        if churn_m is not None and m >= churn_m - 2 and m <= churn_m:
            if rng.random() < 0.6:
                decline = rng.uniform(0.5, 0.9)
        noise = 1.0 + rng.normal(0, noise_std)
        noise = np.clip(noise, 0.3, 1.8)
        base = 100 * h * onboarding_factor * seasonality * decline * noise
        active_users = max(1, int(base * rng.uniform(0.3, 1.0)))
        usage_count = max(0, int(base * active_users * rng.uniform(0.5, 1.5)))
        for feat in features:
            f_noise = 1.0 + rng.normal(0, 0.15)
            rows.append({
                "company_id": company_id,
                "month": calendar_dates[m] if isinstance(calendar_dates[m], str) else pd.Timestamp(calendar_dates[m]).strftime("%Y-%m-%d"),
                "customer_id": cid,
                "feature_key": feat,
                "usage_count": max(0, int(usage_count * np.clip(f_noise, 0.5, 1.5))),
                "active_users": active_users,
            })

    df = pd.DataFrame(rows)
    if not df.empty and "month" in df.columns:
        df["month"] = pd.to_datetime(df["month"]).dt.strftime("%Y-%m-%d")
    return df
