"""Generate customers with segment mix, latents, and observed crm_health_input (with contradictions)."""

from __future__ import annotations

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Any


SEGMENTS = ["enterprise", "large", "medium", "smb"]
REGIONS = ["US", "EU", "DACH", "APAC"]
INDUSTRIES = ["Technology", "Finance", "Healthcare", "Retail", "Manufacturing", "Other"]


def generate_customers(
    config: dict,
    calendar_months: np.ndarray,
    company_id: int = 1,
    rng: np.random.Generator | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if rng is None:
        rng = np.random.default_rng(config.get("random_seed", 42))

    n = int(config.get("n_customers_total", 1200))
    mix = config.get("segment_mix", {})
    probs = [mix.get(s, 0.25) for s in SEGMENTS]
    probs = np.array(probs) / np.sum(probs)

    segments = rng.choice(SEGMENTS, size=n, p=probs)
    created_months = rng.choice(len(calendar_months), size=n)  # index into calendar
    created_dates = [calendar_months[i] if isinstance(calendar_months[i], str) else pd.Timestamp(calendar_months[i]).strftime("%Y-%m-%d") for i in created_months]

    latent_health = rng.uniform(0.2, 0.95, size=n)
    price_sensitivity = rng.uniform(0, 1, size=n)
    expansion_propensity = rng.uniform(0, 1, size=n)
    # Onboarding complexity: enterprise/large higher, mid_smb lower
    onboarding_complexity = np.where(
        np.isin(segments, ["enterprise", "large"]),
        rng.uniform(0.4, 0.9, size=n),
        rng.uniform(0.1, 0.5, size=n),
    )

    # Observed CRM health 1-10 from latent_health with noise
    contradictory_rate = config.get("usage", {}).get("contradictory_signal_rate", 0.08)
    crm_raw = latent_health * 9 + 1  # 0..1 -> 1..10
    noise = rng.normal(0, 0.8, size=n)
    crm_health = np.clip(np.round(crm_raw + noise).astype(int), 1, 10)
    # Contradictions: with probability contradictory_rate, set crm to disagree with latent
    contradict = rng.random(size=n) < contradictory_rate
    if np.any(contradict):
        idx = np.where(contradict)[0]
        # High latent -> give low crm; low latent -> give high crm
        crm_health[idx] = np.where(
            latent_health[idx] >= 0.5,
            rng.integers(1, 4, size=len(idx)),
            rng.integers(7, 11, size=len(idx)),
        )

    df = pd.DataFrame({
        "company_id": company_id,
        "customer_id": np.arange(1, n + 1),
        "customer_name": [f"Customer {i}" for i in range(1, n + 1)],
        "segment": segments,
        "region": rng.choice(REGIONS, size=n),
        "industry": rng.choice(INDUSTRIES, size=n),
        "crm_health_input": crm_health,
        "created_date": created_dates,
    })

    latents = {
        "latent_health": latent_health,
        "price_sensitivity": price_sensitivity,
        "expansion_propensity": expansion_propensity,
        "onboarding_complexity": onboarding_complexity,
        "created_month_index": created_months,
    }
    return df, latents
