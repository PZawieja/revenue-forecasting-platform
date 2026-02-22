"""
Simulation quality validator: checks generated sim data against config realism targets.
Exit 0 if all critical checks pass, 1 otherwise. Warnings do not fail.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from forecasting.sim.src.sim_config import load_config

# Tolerances and bounds (configurable in code)
CHURN_RELATIVE_TOLERANCE = 0.35  # +/- 35% relative vs target
SEGMENT_ABS_TOLERANCE = 0.08     # max absolute diff per segment share
TOP5_SHARE_OVERALL_MIN = 0.20
TOP5_SHARE_OVERALL_MAX = 0.70
TOP5_SHARE_ENTERPRISE_LARGE_MIN = 0.30
PIPELINE_CLOSE_RATE_MIN = 0.15
PIPELINE_CLOSE_RATE_MAX = 0.45
PIPELINE_STAGE_VOLATILITY_MIN = 0.05
USAGE_CV_MIN = 0.15
USAGE_CRM_CORR_MIN = 0.15
USAGE_CRM_CORR_MAX = 0.75


def _repo_root() -> Path:
    # forecasting/sim/src/validate_simulation.py -> src -> sim -> forecasting -> repo
    return Path(__file__).resolve().parent.parent.parent.parent


def _base_path(config: dict) -> Path:
    out = config.get("output", {})
    base = out.get("base_path", "./warehouse/sim_data")
    p = Path(base)
    if not p.is_absolute():
        p = (_repo_root() / base).resolve()
    else:
        p = p.resolve()
    return p


def _load_parquet(base: Path, name: str) -> pd.DataFrame:
    path = base / f"{name}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing: {path}")
    return pd.read_parquet(path)


def _segment_distribution(customers: pd.DataFrame, config: dict) -> tuple[bool, list[str]]:
    """Compare actual segment mix to config.segment_mix. Critical if any absolute diff > SEGMENT_ABS_TOLERANCE."""
    target = config.get("segment_mix", {})
    if not target:
        return True, []
    n = len(customers)
    if n == 0:
        return False, ["No customers"]
    actual = customers["segment"].value_counts(normalize=True).to_dict()
    fails = []
    for seg, target_pct in target.items():
        actual_pct = actual.get(seg, 0.0)
        diff = abs(actual_pct - target_pct)
        if diff > SEGMENT_ABS_TOLERANCE:
            fails.append(f"Segment {seg}: |actual - target| = {diff:.3f} (max {SEGMENT_ABS_TOLERANCE})")
    return len(fails) == 0, fails


def _annualized_churn(subscriptions: pd.DataFrame, customers: pd.DataFrame, config: dict) -> tuple[bool, list[str]]:
    """Approximate annualized logo churn by segment. Critical if outside target +/- CHURN_RELATIVE_TOLERANCE."""
    targets = config.get("churn_targets_by_segment", {})
    if not targets or subscriptions.empty:
        return True, []

    subs = subscriptions.copy()
    subs["contract_start_date"] = pd.to_datetime(subs["contract_start_date"])
    subs["contract_end_date"] = pd.to_datetime(subs["contract_end_date"])
    subs["mrr"] = subs["quantity"] * subs["unit_price"] * (1 - subs["discount_pct"])
    subs.loc[subs["billing_frequency"] == "annual", "mrr"] = subs["mrr"] / 12

    # Build customer-month ARR: for each (customer_id, month) sum mrr*12 from active contracts
    months = pd.date_range(
        start=subs["contract_start_date"].min().to_period("M").to_timestamp(),
        end=subs["contract_end_date"].max().to_period("M").to_timestamp(),
        freq="MS",
    )
    rows = []
    for _, row in subs.iterrows():
        for m in months:
            if row["contract_start_date"] <= m <= row["contract_end_date"] and row["status"] == "active":
                rows.append({"customer_id": row["customer_id"], "month": m, "arr": row["mrr"] * 12})
    if not rows:
        return True, ["No active subscription-months for churn calc"]
    arr_df = pd.DataFrame(rows).groupby(["customer_id", "month"], as_index=False)["arr"].sum()
    arr_df = arr_df.merge(customers[["customer_id", "segment"]], on="customer_id", how="left")

    # Churn event: had ARR > 0, then 0 for >= 2 consecutive months
    arr_wide = arr_df.pivot(index="customer_id", columns="month", values="arr").fillna(0)
    arr_wide = arr_wide.sort_index(axis=1)
    churned = set()
    for cid in arr_wide.index:
        s = arr_wide.loc[cid]
        for i in range(len(s) - 2):
            if s.iloc[i] > 0 and s.iloc[i + 1] == 0 and s.iloc[i + 2] == 0:
                churned.add(cid)
                break
    churned_df = customers[customers["customer_id"].isin(churned)].copy()
    at_risk = arr_wide.sum(axis=1)
    at_risk = at_risk[at_risk > 0]
    n_at_risk = len(at_risk)
    n_churned = len(churned)
    if n_at_risk == 0:
        return True, []
    period_months = len(arr_wide.columns)
    annualized_overall = (n_churned / n_at_risk) * (12 / max(1, period_months / 12))

    # By segment
    fails = []
    churned_ids = set(churned_df["customer_id"])
    for seg in targets:
        seg_customers = set(customers[customers["segment"] == seg]["customer_id"])
        seg_at_risk = seg_customers & set(at_risk.index)
        seg_churned = seg_at_risk & churned_ids
        n_s = len(seg_at_risk)
        if n_s == 0:
            continue
        n_c_s = len(seg_churned)
        rate_s = (n_c_s / n_s) * (12 / max(1, period_months / 12))
        target_s = targets[seg]
        low = target_s * (1 - CHURN_RELATIVE_TOLERANCE)
        high = target_s * (1 + CHURN_RELATIVE_TOLERANCE)
        if rate_s < low or rate_s > high:
            fails.append(f"Churn {seg}: annualized {rate_s:.3f} (target {target_s}, allowed [{low:.3f}, {high:.3f}])")
    return len(fails) == 0, fails


def _revenue_concentration(subscriptions: pd.DataFrame, customers: pd.DataFrame, config: dict) -> tuple[bool, list[str]]:
    """Last simulated month: top 5 share overall and by segment_group. Critical if outside bounds."""
    if subscriptions.empty:
        return True, []
    subs = subscriptions[subscriptions["status"] == "active"].copy()
    subs["mrr"] = subs["quantity"] * subs["unit_price"] * (1 - subs["discount_pct"])
    subs.loc[subs["billing_frequency"] == "annual", "mrr"] = subs["mrr"] / 12
    start = pd.Timestamp(config.get("start_month", "2024-01-01"))
    months = int(config.get("months", 24))
    last_dt = start + pd.DateOffset(months=months - 1)
    subs["end"] = pd.to_datetime(subs["contract_end_date"])
    subs["start"] = pd.to_datetime(subs["contract_start_date"])
    active = subs[(subs["start"] <= last_dt) & (subs["end"] >= last_dt)]
    arr_last = (active.groupby("customer_id")["mrr"].sum() * 12).reset_index()
    arr_last = arr_last.rename(columns={"mrr": "arr"})
    arr_last = arr_last.merge(customers[["customer_id", "segment"]], on="customer_id", how="left")
    arr_last["segment_group"] = arr_last["segment"].apply(
        lambda s: "enterprise_large" if s in ("enterprise", "large") else "mid_smb"
    )

    fails = []
    total_arr = arr_last["arr"].sum()
    if total_arr <= 0:
        return True, []
    top5 = arr_last.nlargest(5, "arr")["arr"].sum()
    share = top5 / total_arr
    if share < TOP5_SHARE_OVERALL_MIN or share > TOP5_SHARE_OVERALL_MAX:
        fails.append(f"Top-5 share overall {share:.2f} (allowed [{TOP5_SHARE_OVERALL_MIN}, {TOP5_SHARE_OVERALL_MAX}])")
    el = arr_last[arr_last["segment_group"] == "enterprise_large"]
    if len(el) >= 5:
        el_total = el["arr"].sum()
        if el_total > 0:
            el_top5 = el.nlargest(5, "arr")["arr"].sum()
            el_share = el_top5 / el_total
            if el_share < TOP5_SHARE_ENTERPRISE_LARGE_MIN:
                fails.append(f"Enterprise_large top-5 share {el_share:.2f} (min {TOP5_SHARE_ENTERPRISE_LARGE_MIN})")
    return len(fails) == 0, fails


def _pipeline_checks(pipeline: pd.DataFrame) -> tuple[bool, list[str]]:
    """Close rate 0.15-0.45; stage volatility (regression) >= 0.05."""
    if pipeline.empty:
        return True, []
    pipeline = pipeline.copy()
    pipeline["snapshot_date"] = pd.to_datetime(pipeline["snapshot_date"])
    # Terminal outcome per opportunity: last snapshot per opp
    last = pipeline.sort_values("snapshot_date").groupby("opportunity_id").last().reset_index()
    closed = last[last["stage"].isin(["closed_won", "closed_lost"])]
    if len(closed) == 0:
        return False, ["No closed_won/closed_lost opportunities"]
    won = (closed["stage"] == "closed_won").sum()
    rate = won / len(closed)
    fails = []
    if rate < PIPELINE_CLOSE_RATE_MIN or rate > PIPELINE_CLOSE_RATE_MAX:
        fails.append(f"Close rate {rate:.2f} (allowed [{PIPELINE_CLOSE_RATE_MIN}, {PIPELINE_CLOSE_RATE_MAX}])")

    # Stage regression: opp moved backward at least once
    stage_order = ["prospecting", "discovery", "proposal", "negotiation", "closed_won", "closed_lost"]
    order_map = {s: i for i, s in enumerate(stage_order)}
    def rank(s):
        return order_map.get(str(s).lower(), -1)
    pipeline["stage_rank"] = pipeline["stage"].apply(rank)
    regressions = 0
    for opp_id, g in pipeline.groupby("opportunity_id"):
        g = g.sort_values("snapshot_date")
        r = g["stage_rank"].tolist()
        for i in range(1, len(r)):
            if r[i] < r[i - 1] and r[i] >= 0:
                regressions += 1
                break
    n_opps = pipeline["opportunity_id"].nunique()
    vol = regressions / n_opps if n_opps else 0
    if vol < PIPELINE_STAGE_VOLATILITY_MIN:
        fails.append(f"Stage volatility (regression %) {vol:.2f} (min {PIPELINE_STAGE_VOLATILITY_MIN})")
    return len(fails) == 0, fails


def _usage_checks(usage: pd.DataFrame, customers: pd.DataFrame) -> tuple[bool, list[str]]:
    """CV(usage_per_user) >= 0.15; correlation(crm_health, avg usage) in [0.15, 0.75]."""
    if usage.empty:
        return True, []
    usage = usage.copy()
    usage["usage_per_user"] = usage["usage_count"] / usage["active_users"].replace(0, float("nan"))
    usage = usage.dropna(subset=["usage_per_user"])
    if usage["usage_per_user"].mean() == 0:
        return False, ["Usage mean is 0"]
    cv = usage["usage_per_user"].std() / usage["usage_per_user"].mean()
    fails = []
    if cv < USAGE_CV_MIN:
        fails.append(f"Usage CV {cv:.2f} (min {USAGE_CV_MIN})")

    avg_usage = usage.groupby("customer_id")["usage_per_user"].mean().reset_index()
    merged = avg_usage.merge(customers[["customer_id", "crm_health_input"]], on="customer_id", how="inner")
    if len(merged) < 10:
        return len(fails) == 0, fails
    corr = merged["usage_per_user"].corr(merged["crm_health_input"])
    if pd.isna(corr):
        fails.append("CRM health vs usage correlation is NaN")
    elif corr < USAGE_CRM_CORR_MIN or corr > USAGE_CRM_CORR_MAX:
        fails.append(f"CRM health vs usage correlation {corr:.2f} (allowed [{USAGE_CRM_CORR_MIN}, {USAGE_CRM_CORR_MAX}])")
    return len(fails) == 0, fails


def run_validation(config_path: str | Path) -> int:
    config = load_config(config_path)
    base = _base_path(config)

    customers = _load_parquet(base, "customers")
    products = _load_parquet(base, "products")
    subscriptions = _load_parquet(base, "subscription_line_items")
    usage = _load_parquet(base, "usage_monthly")
    pipeline = _load_parquet(base, "pipeline_opportunities_snapshot")

    all_fails = []
    warnings = []

    print("Simulation quality validator")
    print("===========================")

    ok, errs = _segment_distribution(customers, config)
    if errs:
        print("\n[Segment distribution]")
        for e in errs:
            print(f"  FAIL: {e}")
            all_fails.extend(errs)
    else:
        print("\n[Segment distribution] OK")

    ok, errs = _annualized_churn(subscriptions, customers, config)
    if errs:
        print("\n[Churn by segment]")
        for e in errs:
            print(f"  FAIL: {e}")
            all_fails.extend(errs)
    else:
        print("\n[Churn by segment] OK")

    ok, errs = _revenue_concentration(subscriptions, customers, config)
    if errs:
        print("\n[Revenue concentration]")
        for e in errs:
            print(f"  FAIL: {e}")
            all_fails.extend(errs)
    else:
        print("\n[Revenue concentration] OK")

    ok, errs = _pipeline_checks(pipeline)
    if errs:
        print("\n[Pipeline]")
        for e in errs:
            print(f"  FAIL: {e}")
            all_fails.extend(errs)
    else:
        print("\n[Pipeline] OK")

    ok, errs = _usage_checks(usage, customers)
    if errs:
        print("\n[Usage]")
        for e in errs:
            print(f"  FAIL: {e}")
            all_fails.extend(errs)
    else:
        print("\n[Usage] OK")

    print("\n===========================")
    if all_fails:
        print("Result: FAILED (critical checks)")
        return 1
    if warnings:
        print("Result: PASSED with warnings")
    else:
        print("Result: PASSED")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate sim data against config realism targets.")
    parser.add_argument(
        "--config",
        default="forecasting/sim/config/sim_config.yml",
        help="Path to sim_config.yml",
    )
    args = parser.parse_args()
    return run_validation(args.config)


if __name__ == "__main__":
    sys.exit(main())
