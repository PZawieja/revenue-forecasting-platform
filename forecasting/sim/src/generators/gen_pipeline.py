"""Generate pipeline_opportunities_snapshot: opportunity_id x snapshot_date with stage progression and slippage."""

from __future__ import annotations

import pandas as pd
import numpy as np
from typing import Any

COMPANY_ID = 1
STAGE_ORDER = ["prospecting", "discovery", "proposal", "negotiation", "closed_won", "closed_lost"]


def generate_pipeline(
    config: dict,
    calendar_dates: list,
    customers_df: pd.DataFrame,
    company_id: int = 1,
    rng: np.random.Generator | None = None,
) -> pd.DataFrame:
    if rng is None:
        rng = np.random.default_rng(config.get("random_seed", 42))

    pipe_cfg = config.get("pipeline", {})
    opps_per_100 = pipe_cfg.get("opps_per_month_per_100_customers", 8)
    stage_names = pipe_cfg.get("stage_names", STAGE_ORDER)
    slippage_cfg = pipe_cfg.get("slippage_by_stage_months", {})
    months = len(calendar_dates)
    n_customers = len(customers_df)
    segments = customers_df["segment"].values
    customer_ids = customers_df["customer_id"].values

    opp_counter = [0]

    def next_opp_id():
        opp_counter[0] += 1
        return f"opp{opp_counter[0]}"

    def amount_for_segment(seg: str):
        if seg == "enterprise":
            return 50000 + 150000 * (rng.random() ** 0.6)
        if seg == "large":
            return 20000 + 80000 * (rng.random() ** 0.5)
        if seg == "medium":
            return 5000 + 25000 * (rng.random() ** 0.5)
        return 1000 + 8000 * (rng.random() ** 0.6)

    def get_slippage(seg: str, stage: str) -> int:
        d = slippage_cfg.get("enterprise", {}) if seg in ("enterprise", "large") else slippage_cfg.get("mid_smb", {})
        return d.get(stage, 0)

    rows = []
    open_opps = []

    for m in range(months):
        snap_date = pd.Timestamp(calendar_dates[m]).strftime("%Y-%m-%d")
        n_new = max(0, int(n_customers * opps_per_100 / 100 * (0.8 + 0.4 * rng.random())))
        for _ in range(n_new):
            is_expansion = rng.random() < 0.4
            seg = str(rng.choice(segments))
            exp_close = (pd.Timestamp(calendar_dates[m]) + pd.DateOffset(months=3)).strftime("%Y-%m-%d")
            open_opps.append({
                "opportunity_id": next_opp_id(),
                "customer_id": int(rng.choice(customer_ids)) if is_expansion else None,
                "segment": seg,
                "opportunity_type": "expansion" if is_expansion else "new_biz",
                "stage": "prospecting",
                "amount": round(amount_for_segment(seg), 2),
                "expected_close_date": exp_close,
            })

        still_open = []
        # In the last 6 months, force-close some open opps at proposal/negotiation so validator sees closed_won/closed_lost
        force_close_window = months - 6 <= m
        for o in open_opps:
            if o["stage"] in ("closed_won", "closed_lost"):
                rows.append({
                    "company_id": company_id,
                    "snapshot_date": snap_date,
                    "opportunity_id": o["opportunity_id"],
                    "customer_id": o["customer_id"],
                    "segment": o["segment"],
                    "stage": o["stage"],
                    "amount": o["amount"],
                    "expected_close_date": o["expected_close_date"],
                    "opportunity_type": o["opportunity_type"],
                })
                continue
            idx = stage_names.index(o["stage"]) if o["stage"] in stage_names else 0
            # Force-close a fraction of late-stage opps in the last 6 months so we have closed outcomes
            if force_close_window and o["stage"] in ("proposal", "negotiation") and rng.random() < 0.35:
                o["stage"] = "closed_won" if rng.random() < 0.65 else "closed_lost"
            elif rng.random() < 0.52:
                if idx < len(stage_names) - 2:
                    o["stage"] = stage_names[idx + 1]
                    slip = get_slippage(o["segment"], o["stage"])
                    o["expected_close_date"] = (pd.Timestamp(o["expected_close_date"]) + pd.DateOffset(months=slip)).strftime("%Y-%m-%d")
                elif idx == len(stage_names) - 2:
                    o["stage"] = "closed_won" if rng.random() < 0.65 else "closed_lost"
            elif o["stage"] == "negotiation" and rng.random() < 0.12:
                o["stage"] = "closed_lost"
            # Append row with current (possibly updated) stage so closed_won/closed_lost appear in output
            rows.append({
                "company_id": company_id,
                "snapshot_date": snap_date,
                "opportunity_id": o["opportunity_id"],
                "customer_id": o["customer_id"],
                "segment": o["segment"],
                "stage": o["stage"],
                "amount": o["amount"],
                "expected_close_date": o["expected_close_date"],
                "opportunity_type": o["opportunity_type"],
            })
            if o["stage"] not in ("closed_won", "closed_lost"):
                still_open.append(o)
        open_opps = still_open

    return pd.DataFrame(rows)
