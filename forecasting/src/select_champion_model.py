"""
Champion/challenger model selection: choose preferred_model (logistic vs xgboost) per dataset
from backtest performance and stability, then write ml_model_selection to DuckDB for dbt.

Selection logic (deterministic):
- For each dataset (renewals, pipeline), over the latest 6 cutoff_months:
  - mean_logloss, std_logloss, mean_brier, std_brier per model.
- Composite score (lower is better):
  score = mean_logloss + 0.5*mean_brier + 0.25*std_logloss + 0.10*std_brier
- Choose model with lowest score.
- Stability guardrail: if the best model beats the other by less than 1% on score,
  choose logistic (prefer simpler model).
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from forecasting.src.io_duckdb import read_table, write_table

LATEST_N_CUTOFFS = 6
# Composite score weights (lower is better)
W_MEAN_LOGLOSS = 1.0
W_MEAN_BRIER = 0.5
W_STD_LOGLOSS = 0.25
W_STD_BRIER = 0.10
# If best model leads by less than this fraction, choose logistic (stability guardrail)
STABILITY_MARGIN = 0.01


def _warehouse_dir_from_duckdb_path(duckdb_path: str | Path) -> Path:
    return Path(duckdb_path).resolve().parent


def _latest_n_cutoffs_per_model(
    df: pd.DataFrame,
    n: int = LATEST_N_CUTOFFS,
) -> pd.DataFrame:
    """Filter to segment='all' if present, then keep latest n cutoff_months per model_name."""
    if "segment" in df.columns:
        df = df.loc[df["segment"] == "all"].copy()
    df = df.sort_values("cutoff_month", ascending=False)
    cutoffs = df["cutoff_month"].unique()[:n]
    return df[df["cutoff_month"].isin(cutoffs)]


def _scores_for_dataset(df: pd.DataFrame) -> dict[str, float]:
    """Compute composite score per model. Returns dict model_name -> score (lower is better)."""
    out = {}
    for model_name, grp in df.groupby("model_name"):
        mean_ll = grp["logloss"].mean()
        std_ll = grp["logloss"].std()
        if pd.isna(std_ll):
            std_ll = 0.0
        mean_b = grp["brier"].mean()
        std_b = grp["brier"].std()
        if pd.isna(std_b):
            std_b = 0.0
        score = (
            W_MEAN_LOGLOSS * mean_ll
            + W_MEAN_BRIER * mean_b
            + W_STD_LOGLOSS * std_ll
            + W_STD_BRIER * std_b
        )
        out[model_name] = float(score)
    return out


def _choose_champion(
    score_logistic: Optional[float],
    score_xgboost: Optional[float],
) -> tuple[str, str]:
    """
    Choose preferred_model and selection_reason.
    If only one model has a score, choose it. Else pick lower score; apply stability guardrail.
    """
    if score_logistic is None and score_xgboost is None:
        return "logistic", "no_backtest_data_default"
    if score_logistic is None:
        return "xgboost", "single_model"
    if score_xgboost is None:
        return "logistic", "single_model"
    if score_logistic <= score_xgboost:
        best, other = "logistic", score_xgboost
        other_name = "xgboost"
    else:
        best, other = "xgboost", score_logistic
        other_name = "logistic"
    best_score = score_logistic if best == "logistic" else score_xgboost
    # Stability guardrail: if best leads by < 1%, prefer logistic
    if other > 0 and (other - best_score) / other < STABILITY_MARGIN:
        return "logistic", "stability_guardrail"
    return best, "champion"


def run_selection(warehouse_dir: Optional[Path] = None) -> None:
    renewal = read_table("SELECT * FROM main.ml_renewal_backtest_metrics", warehouse_dir=warehouse_dir)
    pipeline = read_table("SELECT * FROM main.ml_pipeline_backtest_metrics", warehouse_dir=warehouse_dir)

    renewal = _latest_n_cutoffs_per_model(renewal)
    pipeline = _latest_n_cutoffs_per_model(pipeline)

    rows = []
    for dataset, df in [("renewals", renewal), ("pipeline", pipeline)]:
        scores = _scores_for_dataset(df)
        score_logistic = scores.get("logistic")
        score_xgboost = scores.get("xgboost")
        preferred_model, selection_reason = _choose_champion(score_logistic, score_xgboost)
        rows.append({
            "dataset": dataset,
            "preferred_model": preferred_model,
            "selection_reason": selection_reason,
            "score_logistic": score_logistic,
            "score_xgboost": score_xgboost,
            "updated_at_utc": datetime.now(timezone.utc),
        })

    out = pd.DataFrame(rows)
    write_table(out, "ml_model_selection", mode="replace", warehouse_dir=warehouse_dir)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Select champion model per dataset from backtest metrics and write ml_model_selection.",
    )
    parser.add_argument(
        "--duckdb-path",
        type=str,
        default="./warehouse/revenue_forecasting.duckdb",
        help="Path to DuckDB database file",
    )
    args = parser.parse_args()
    warehouse_dir = _warehouse_dir_from_duckdb_path(args.duckdb_path)
    run_selection(warehouse_dir=warehouse_dir)


if __name__ == "__main__":
    main()
