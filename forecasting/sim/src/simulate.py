"""
Realistic simulation mode: generate Parquet datasets for DuckDB/dbt consumption.
Deterministic when random_seed is set. Outputs to config output.base_path.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from forecasting.sim.src.io import write_parquet
from forecasting.sim.src.sim_config import load_config
from forecasting.sim.src.generators.gen_products import generate_products
from forecasting.sim.src.generators.gen_customers import generate_customers
from forecasting.sim.src.generators.gen_subscriptions import generate_subscriptions
from forecasting.sim.src.generators.gen_usage import generate_usage
from forecasting.sim.src.generators.gen_pipeline import generate_pipeline

COMPANY_ID = 1


def _month_calendar(config: dict) -> list:
    start = config.get("start_month", "2024-01-01")
    months = int(config.get("months", 24))
    dr = pd.date_range(start=start, periods=months, freq="MS")
    return [d.strftime("%Y-%m-%d") for d in dr]


def _quality_report(
    config: dict,
    customers_df: pd.DataFrame,
    subscriptions_df: pd.DataFrame,
    pipeline_df: pd.DataFrame,
) -> None:
    n_customers = len(customers_df)
    seg_counts = customers_df["segment"].value_counts()
    print("\n--- Data quality report ---")
    print("Customers per segment:")
    for seg in ["enterprise", "large", "medium", "smb"]:
        c = seg_counts.get(seg, 0)
        print(f"  {seg}: {c}")

    subs = subscriptions_df.copy()
    if subs.empty:
        print("Churn/ARR: no subscriptions.")
        print("Pipeline: no data.")
        return

    # MRR for churn/ARR metrics
    subs["mrr"] = subs["quantity"] * subs["unit_price"] * (1 - subs["discount_pct"])
    subs["mrr"] = np.where(subs["billing_frequency"] == "annual", subs["mrr"] / 12, subs["mrr"])
    cancelled = subs[subscriptions_df["status"] == "cancelled"]
    active = subs[subscriptions_df["status"] == "active"]
    n_contracts = subs["contract_id"].nunique()
    n_cancelled = subscriptions_df[subscriptions_df["status"] == "cancelled"]["contract_id"].nunique()
    churn_logo = n_cancelled / n_contracts if n_contracts else 0
    rev_cancelled = cancelled["mrr"].sum()
    rev_total = subs["mrr"].sum()
    churn_rev = rev_cancelled / rev_total if rev_total else 0
    print(f"Churn (logo): {churn_logo:.2%} of contracts cancelled")
    print(f"Churn (revenue): {churn_rev:.2%} of contract MRR lost")

    arr_by_seg = subs.merge(customers_df[["customer_id", "segment"]], on="customer_id", how="left")
    arr_by_seg["arr"] = arr_by_seg["mrr"] * 12
    avg_arr = arr_by_seg.groupby("segment")["arr"].mean()
    print("Avg ARR by segment:")
    for seg in ["enterprise", "large", "medium", "smb"]:
        a = avg_arr.get(seg, 0)
        print(f"  {seg}: {a:.0f}")

    if not pipeline_df.empty:
        closed = pipeline_df[pipeline_df["stage"].isin(["closed_won", "closed_lost"])]
        if len(closed) > 0:
            last_snap = closed.drop_duplicates("opportunity_id", keep="last")
            won = (last_snap["stage"] == "closed_won").sum()
            total = len(last_snap)
            print(f"Pipeline close rate (of closed opps): {won / total:.1%} won" if total else "N/A")
    print("Sanity counts:")
    print(f"  customers: {n_customers}, subscription_line_items: {len(subs)}, pipeline_snapshots: {len(pipeline_df)}")
    print("---\n")


def run(config_path: str | Path) -> None:
    config = load_config(config_path)
    seed = config.get("random_seed", 42)
    rng = np.random.default_rng(seed)

    out = config.get("output", {})
    base_path = Path(out.get("base_path", "./warehouse/sim_data")).resolve()
    fmt = out.get("format", "parquet")
    if fmt != "parquet":
        raise ValueError(f"Unsupported output format: {fmt}")

    calendar_dates = _month_calendar(config)
    months = len(calendar_dates)

    products_df = generate_products(config, company_id=COMPANY_ID, rng=rng)
    customers_df, latents = generate_customers(config, calendar_dates, company_id=COMPANY_ID, rng=rng)
    subscriptions_df = generate_subscriptions(
        config, calendar_dates, products_df, customers_df, latents, company_id=COMPANY_ID, rng=rng
    )
    usage_df = generate_usage(
        config, calendar_dates, customers_df, subscriptions_df, latents, company_id=COMPANY_ID, rng=rng
    )
    pipeline_df = generate_pipeline(config, calendar_dates, customers_df, company_id=COMPANY_ID, rng=rng)

    # Persist
    write_parquet(products_df, base_path / "products.parquet")
    write_parquet(customers_df, base_path / "customers.parquet")
    write_parquet(subscriptions_df, base_path / "subscription_line_items.parquet")
    write_parquet(usage_df, base_path / "usage_monthly.parquet")
    write_parquet(pipeline_df, base_path / "pipeline_opportunities_snapshot.parquet")

    print(f"Wrote {base_path}/ (products, customers, subscription_line_items, usage_monthly, pipeline_opportunities_snapshot)")
    _quality_report(config, customers_df, subscriptions_df, pipeline_df)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate simulation dataset (Parquet) for DuckDB/dbt.")
    parser.add_argument(
        "--config",
        default="forecasting/sim/config/sim_config.yml",
        help="Path to sim_config.yml",
    )
    args = parser.parse_args()
    run(args.config)


if __name__ == "__main__":
    main()
