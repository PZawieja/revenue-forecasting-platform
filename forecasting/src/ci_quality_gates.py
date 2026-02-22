"""
CI quality gates for ML backtest metrics. Read backtest_metrics tables, evaluate Brier and logloss
against configurable thresholds per dataset. Fail CI only if both logistic and xgboost fail for a dataset.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

from forecasting.src.io_duckdb import read_table

# Default thresholds (mild for synthetic/CI; tune for production)
RENEWALS_BRIER_MAX = 0.25
RENEWALS_LOGLOSS_MAX = 0.75
PIPELINE_BRIER_MAX = 0.30
PIPELINE_LOGLOSS_MAX = 0.80


def _warehouse_dir_from_duckdb_path(duckdb_path: str | Path) -> Path:
    return Path(duckdb_path).resolve().parent


def _latest_per_model(df: pd.DataFrame) -> pd.DataFrame:
    """Keep one row per model_name with the latest cutoff_month (segment='all' if present)."""
    if "segment" in df.columns:
        df = df[df["segment"] == "all"].copy()
    df = df.sort_values("cutoff_month", ascending=False)
    return df.groupby("model_name", as_index=False).first()


def run_gates(
    warehouse_dir: Optional[Path] = None,
    renewals_brier_max: float = RENEWALS_BRIER_MAX,
    renewals_logloss_max: float = RENEWALS_LOGLOSS_MAX,
    pipeline_brier_max: float = PIPELINE_BRIER_MAX,
    pipeline_logloss_max: float = PIPELINE_LOGLOSS_MAX,
) -> bool:
    """
    Evaluate quality gates. Returns True if CI should pass, False if it should fail.
    Prints warnings when one model fails and the other passes.
    """
    try:
        renewal_metrics = read_table(
            "SELECT * FROM main.ml_renewal_backtest_metrics",
            warehouse_dir=warehouse_dir,
        )
    except Exception as e:
        print("ml_renewal_backtest_metrics not found or not readable.", file=sys.stderr)
        raise SystemExit(1) from e
    try:
        pipeline_metrics = read_table(
            "SELECT * FROM main.ml_pipeline_backtest_metrics",
            warehouse_dir=warehouse_dir,
        )
    except Exception as e:
        print("ml_pipeline_backtest_metrics not found or not readable.", file=sys.stderr)
        raise SystemExit(1) from e

    renewal_latest = _latest_per_model(renewal_metrics)
    pipeline_latest = _latest_per_model(pipeline_metrics)

    failed_datasets: list[str] = []
    for _, row in renewal_latest.iterrows():
        model = row["model_name"]
        brier = float(row["brier"])
        logloss = float(row["logloss"])
        b_ok = brier <= renewals_brier_max
        l_ok = logloss <= renewals_logloss_max
        if not (b_ok and l_ok):
            print(
                f"[renewals] {model}: brier={brier:.4f} (max {renewals_brier_max}), logloss={logloss:.4f} (max {renewals_logloss_max}) — FAIL",
                file=sys.stderr,
            )
        else:
            print(f"[renewals] {model}: brier={brier:.4f}, logloss={logloss:.4f} — OK")

    renewal_models = set(renewal_latest["model_name"])
    renewal_failed = {
        row["model_name"]
        for _, row in renewal_latest.iterrows()
        if float(row["brier"]) > renewals_brier_max or float(row["logloss"]) > renewals_logloss_max
    }
    if renewal_failed and renewal_models - renewal_failed:
        print("Warning: renewals — one model passed, one failed; CI passes but consider tuning.", file=sys.stderr)
    if len(renewal_failed) == len(renewal_models) and renewal_models:
        failed_datasets.append("renewals")

    for _, row in pipeline_latest.iterrows():
        model = row["model_name"]
        brier = float(row["brier"])
        logloss = float(row["logloss"])
        b_ok = brier <= pipeline_brier_max
        l_ok = logloss <= pipeline_logloss_max
        if not (b_ok and l_ok):
            print(
                f"[pipeline] {model}: brier={brier:.4f} (max {pipeline_brier_max}), logloss={logloss:.4f} (max {pipeline_logloss_max}) — FAIL",
                file=sys.stderr,
            )
        else:
            print(f"[pipeline] {model}: brier={brier:.4f}, logloss={logloss:.4f} — OK")

    pipeline_models = set(pipeline_latest["model_name"])
    pipeline_failed = {
        row["model_name"]
        for _, row in pipeline_latest.iterrows()
        if float(row["brier"]) > pipeline_brier_max or float(row["logloss"]) > pipeline_logloss_max
    }
    if pipeline_failed and pipeline_models - pipeline_failed:
        print("Warning: pipeline — one model passed, one failed; CI passes but consider tuning.", file=sys.stderr)
    if len(pipeline_failed) == len(pipeline_models) and pipeline_models:
        failed_datasets.append("pipeline")

    if failed_datasets:
        print(f"Quality gates failed for: {', '.join(failed_datasets)} (both models exceeded thresholds).", file=sys.stderr)
        return False
    return True


def main() -> None:
    parser = argparse.ArgumentParser(
        description="CI quality gates: fail if both models exceed Brier/logloss thresholds for a dataset.",
    )
    parser.add_argument(
        "--duckdb-path",
        type=str,
        default="./warehouse/revenue_forecasting.duckdb",
        help="Path to DuckDB database file",
    )
    parser.add_argument(
        "--renewals-brier-max",
        type=float,
        default=RENEWALS_BRIER_MAX,
        help=f"Max Brier for renewals (default {RENEWALS_BRIER_MAX})",
    )
    parser.add_argument(
        "--renewals-logloss-max",
        type=float,
        default=RENEWALS_LOGLOSS_MAX,
        help=f"Max logloss for renewals (default {RENEWALS_LOGLOSS_MAX})",
    )
    parser.add_argument(
        "--pipeline-brier-max",
        type=float,
        default=PIPELINE_BRIER_MAX,
        help=f"Max Brier for pipeline (default {PIPELINE_BRIER_MAX})",
    )
    parser.add_argument(
        "--pipeline-logloss-max",
        type=float,
        default=PIPELINE_LOGLOSS_MAX,
        help=f"Max logloss for pipeline (default {PIPELINE_LOGLOSS_MAX})",
    )
    args = parser.parse_args()
    warehouse_dir = _warehouse_dir_from_duckdb_path(args.duckdb_path)
    ok = run_gates(
        warehouse_dir=warehouse_dir,
        renewals_brier_max=args.renewals_brier_max,
        renewals_logloss_max=args.renewals_logloss_max,
        pipeline_brier_max=args.pipeline_brier_max,
        pipeline_logloss_max=args.pipeline_logloss_max,
    )
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
