"""
Portfolio-grade calibration and business-impact reporting for ML probability models.
Reads backtest result tables, produces calibration bins, threshold metrics, and cost curves; writes to DuckDB.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from forecasting.src.io_duckdb import read_table, write_table

# Cost assumptions (fn_cost, fp_cost) per dataset
RENEWALS_FN_COST = 5
RENEWALS_FP_COST = 1
PIPELINE_FP_COST = 3
PIPELINE_FN_COST = 2

THRESHOLDS = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
N_BINS = 10


def _warehouse_dir_from_duckdb_path(duckdb_path: str | Path) -> Path:
    return Path(duckdb_path).resolve().parent


def _load_backtest_results(
    warehouse_dir: Optional[Path],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load renewal and pipeline backtest results. Exits with helpful message if either table is missing."""
    try:
        renewal = read_table("SELECT * FROM main.ml_renewal_backtest_results", warehouse_dir=warehouse_dir)
    except Exception as e:
        print("ml_renewal_backtest_results not found or not readable.", file=sys.stderr)
        print("Run backtests first: ./scripts/ml_backtest_renewals.sh", file=sys.stderr)
        raise SystemExit(1) from e
    try:
        pipeline = read_table("SELECT * FROM main.ml_pipeline_backtest_results", warehouse_dir=warehouse_dir)
    except Exception as e:
        print("ml_pipeline_backtest_results not found or not readable.", file=sys.stderr)
        print("Run backtests first: ./scripts/ml_backtest_pipeline.sh", file=sys.stderr)
        raise SystemExit(1) from e
    return renewal, pipeline


def _calibration_bins_for_group(df: pd.DataFrame) -> pd.DataFrame:
    """Build 10 probability bins [0,0.1), [0.1,0.2), ..., [0.9,1.0] for one (dataset, model_name, cutoff_month) group."""
    p = np.clip(df["p_pred"].values.astype(float), 0.0, 1.0)
    y = df["y_true"].values.astype(int)
    dataset = df["dataset"].iloc[0]
    model_name = df["model_name"].iloc[0]
    cutoff_month = df["cutoff_month"].iloc[0]

    # Fixed bins: bin_id 1 = [0, 0.1), ..., 10 = [0.9, 1.0]
    bin_id_arr = np.clip(np.floor(p * N_BINS).astype(int), 0, N_BINS - 1) + 1
    bin_id_arr[p >= 1.0] = N_BINS

    rows = []
    for b in range(1, N_BINS + 1):
        mask = bin_id_arr == b
        if mask.sum() == 0:
            continue
        p_bin = p[mask]
        y_bin = y[mask]
        rows.append({
            "dataset": dataset,
            "model_name": model_name,
            "cutoff_month": cutoff_month,
            "bin_id": b,
            "p_pred_mean": float(np.mean(p_bin)),
            "y_true_rate": float(np.mean(y_bin)),
            "count": int(mask.sum()),
        })
    return pd.DataFrame(rows)


def _threshold_metrics_for_group(df: pd.DataFrame) -> pd.DataFrame:
    """For one (dataset, model_name, cutoff_month) group, compute metrics at each threshold."""
    p = df["p_pred"].values.astype(float)
    y = df["y_true"].values.astype(int)
    dataset = df["dataset"].iloc[0]
    model_name = df["model_name"].iloc[0]
    cutoff_month = df["cutoff_month"].iloc[0]

    rows = []
    for thresh in THRESHOLDS:
        pred_pos = (p >= thresh).astype(int)
        tp = int(((pred_pos == 1) & (y == 1)).sum())
        fp = int(((pred_pos == 1) & (y == 0)).sum())
        tn = int(((pred_pos == 0) & (y == 0)).sum())
        fn = int(((pred_pos == 0) & (y == 1)).sum())
        n_pos = pred_pos.sum()
        n_actual_pos = y.sum()
        n_actual_neg = len(y) - n_actual_pos
        precision = tp / n_pos if n_pos else 0.0
        recall = tp / n_actual_pos if n_actual_pos else 0.0
        fpr = fp / n_actual_neg if n_actual_neg else 0.0
        fnr = fn / n_actual_pos if n_actual_pos else 0.0
        rows.append({
            "dataset": dataset,
            "model_name": model_name,
            "cutoff_month": cutoff_month,
            "threshold": thresh,
            "predicted_positive": int(n_pos),
            "tp": tp,
            "fp": fp,
            "tn": tn,
            "fn": fn,
            "precision": precision,
            "recall": recall,
            "fpr": fpr,
            "fnr": fnr,
        })
    return pd.DataFrame(rows)


def _cost_curve_for_group(
    df: pd.DataFrame,
    fn_cost: float,
    fp_cost: float,
) -> pd.DataFrame:
    """Expected cost at each threshold for one group."""
    metrics = _threshold_metrics_for_group(df)
    dataset = df["dataset"].iloc[0]
    model_name = df["model_name"].iloc[0]
    cutoff_month = df["cutoff_month"].iloc[0]
    rows = []
    for _, row in metrics.iterrows():
        expected_cost = row["fn"] * fn_cost + row["fp"] * fp_cost
        rows.append({
            "dataset": dataset,
            "model_name": model_name,
            "cutoff_month": cutoff_month,
            "threshold": row["threshold"],
            "expected_cost": expected_cost,
        })
    return pd.DataFrame(rows)


def run_reports(warehouse_dir: Optional[Path] = None) -> None:
    renewal_df, pipeline_df = _load_backtest_results(warehouse_dir)

    renewal_df = renewal_df.copy()
    renewal_df["dataset"] = "renewals"
    pipeline_df = pipeline_df.copy()
    pipeline_df["dataset"] = "pipeline"

    # Normalize date columns for grouping
    for col in ("cutoff_month",):
        if col in renewal_df.columns:
            renewal_df[col] = pd.to_datetime(renewal_df[col]).dt.normalize()
        if col in pipeline_df.columns:
            pipeline_df[col] = pd.to_datetime(pipeline_df[col]).dt.normalize()

    # Calibration bins
    all_bins = []
    for (dataset, model_name, cutoff_month), grp in pd.concat([renewal_df, pipeline_df], ignore_index=True).groupby(
        ["dataset", "model_name", "cutoff_month"]
    ):
        all_bins.append(_calibration_bins_for_group(grp))
    bins_df = pd.concat(all_bins, ignore_index=True)
    write_table(bins_df, "ml_calibration_bins", mode="replace", warehouse_dir=warehouse_dir)

    # Threshold metrics
    all_metrics = []
    for (dataset, model_name, cutoff_month), grp in pd.concat([renewal_df, pipeline_df], ignore_index=True).groupby(
        ["dataset", "model_name", "cutoff_month"]
    ):
        all_metrics.append(_threshold_metrics_for_group(grp))
    metrics_df = pd.concat(all_metrics, ignore_index=True)
    write_table(metrics_df, "ml_threshold_metrics", mode="replace", warehouse_dir=warehouse_dir)

    # Cost curves
    all_costs = []
    for (dataset, model_name, cutoff_month), grp in renewal_df.groupby(["dataset", "model_name", "cutoff_month"]):
        all_costs.append(_cost_curve_for_group(grp, fn_cost=RENEWALS_FN_COST, fp_cost=RENEWALS_FP_COST))
    for (dataset, model_name, cutoff_month), grp in pipeline_df.groupby(["dataset", "model_name", "cutoff_month"]):
        all_costs.append(_cost_curve_for_group(grp, fn_cost=PIPELINE_FN_COST, fp_cost=PIPELINE_FP_COST))
    cost_df = pd.concat(all_costs, ignore_index=True)
    write_table(cost_df, "ml_cost_curves", mode="replace", warehouse_dir=warehouse_dir)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build ML calibration and business-impact reports from backtest results.",
    )
    parser.add_argument(
        "--duckdb-path",
        type=str,
        default="./warehouse/revenue_forecasting.duckdb",
        help="Path to DuckDB database file",
    )
    args = parser.parse_args()
    warehouse_dir = _warehouse_dir_from_duckdb_path(args.duckdb_path)
    run_reports(warehouse_dir=warehouse_dir)


if __name__ == "__main__":
    main()
