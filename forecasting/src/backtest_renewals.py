"""
Walk-forward backtesting for renewal ML: train on past, predict on each of the last 6 renewal months,
write per-row results and aggregate metrics (with segment breakdown) to DuckDB.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.metrics import log_loss, roc_auc_score

from forecasting.src.io_duckdb import read_table, write_table
from forecasting.src.train_renewals import (
    CAT_COLS,
    NUM_COLS,
    TARGET,
    load_features,
    prepare_features,
    train_logistic,
    train_xgboost,
)

try:
    import xgboost as xgb
except ImportError:
    xgb = None

# Number of most recent renewal months to use as cutoffs (test months)
DEFAULT_LAST_N_CUTOFFS = 6


def _warehouse_dir_from_duckdb_path(duckdb_path: str | Path) -> Path:
    """Derive warehouse directory from full DuckDB file path."""
    return Path(duckdb_path).resolve().parent


def _brier(y_true: np.ndarray, p_pred: np.ndarray) -> float:
    return float(np.mean((np.asarray(p_pred) - np.asarray(y_true)) ** 2))


def _evaluate(y_true: np.ndarray, p_pred: np.ndarray) -> dict[str, float]:
    """AUC, Brier, LogLoss. p_pred clipped for logloss."""
    p_pred = np.clip(np.asarray(p_pred, dtype=float), 1e-7, 1 - 1e-7)
    y_true = np.asarray(y_true, dtype=float)
    n_unique = len(np.unique(y_true))
    return {
        "auc": float(roc_auc_score(y_true, p_pred)) if n_unique > 1 else 0.0,
        "brier": _brier(y_true, p_pred),
        "logloss": float(log_loss(y_true, p_pred)),
    }


def _wape_like_classification(brier: float, logloss: float) -> float:
    """Single scalar for classification 'error' (no WAPE); use Brier as primary."""
    return brier


def run_backtest(
    warehouse_dir: Optional[Path] = None,
    last_n_cutoffs: int = DEFAULT_LAST_N_CUTOFFS,
    models_to_run: Optional[list[str]] = None,
) -> None:
    """
    Walk-forward backtest: for each cutoff_month in the last N renewal months,
    train on renewal_month < cutoff_month, predict on renewal_month == cutoff_month.
    Write ml_renewal_backtest_results and ml_renewal_backtest_metrics to DuckDB.
    """
    if models_to_run is None:
        models_to_run = ["logistic", "xgboost"]

    df = load_features(warehouse_dir)
    df = df.sort_values("renewal_month").reset_index(drop=True)
    renewal_months = pd.Series(df["renewal_month"].unique()).sort_values()
    if len(renewal_months) < 1:
        raise ValueError("Need at least one renewal_month in ml_features_renewals.")

    cutoff_months = renewal_months.tail(last_n_cutoffs).tolist()
    if not cutoff_months:
        raise ValueError("No cutoff months; reduce last_n_cutoffs or add more data.")

    all_results: list[pd.DataFrame] = []
    all_metrics: list[pd.DataFrame] = []

    for cutoff_month in cutoff_months:
        train_df = df[df["renewal_month"] < cutoff_month]
        test_df = df[df["renewal_month"] == cutoff_month]
        if test_df.empty:
            continue

        y_test = test_df[TARGET].values
        segment_test = (
            test_df["segment"].fillna("unknown").astype(str).values
            if "segment" in test_df.columns
            else np.full(len(test_df), "all")
        )

        # Prepare features: fit on train, transform test
        X_train_scaled, enc, scaler = prepare_features(train_df, scale=True)
        y_train = train_df[TARGET].values
        X_test_scaled, _, _ = prepare_features(test_df, fit_encoder=enc, fit_scaler=scaler, scale=True)

        X_train_raw, enc_raw, _ = prepare_features(train_df, scale=False)
        X_test_raw, _, _ = prepare_features(test_df, fit_encoder=enc_raw, scale=False)

        for model_name in models_to_run:
            if model_name == "logistic":
                model, _ = train_logistic(X_train_scaled, y_train, X_test_scaled, y_test)
                p_pred = model.predict_proba(X_test_scaled)[:, 1]
            elif model_name == "xgboost":
                if xgb is None:
                    continue
                model, _ = train_xgboost(X_train_raw, y_train, X_test_raw, y_test)
                p_pred = model.predict_proba(X_test_raw)[:, 1]
            else:
                continue

            # Per-row results: cutoff_month, model_name, company_id, customer_id, renewal_month, y_true, p_pred
            res = pd.DataFrame({
                "cutoff_month": cutoff_month,
                "model_name": model_name,
                "company_id": test_df["company_id"].values,
                "customer_id": test_df["customer_id"].values,
                "renewal_month": test_df["renewal_month"].values,
                "y_true": y_test.astype(int),
                "p_pred": p_pred.astype(float),
            })
            all_results.append(res)

            # Overall metrics for this (cutoff_month, model_name)
            ev = _evaluate(y_test, p_pred)
            wape_like = _wape_like_classification(ev["brier"], ev["logloss"])
            all_metrics.append(
                pd.DataFrame(
                    [{
                        "cutoff_month": cutoff_month,
                        "model_name": model_name,
                        "segment": "all",
                        "wape_like": wape_like,
                        "auc": ev["auc"],
                        "brier": ev["brier"],
                        "logloss": ev["logloss"],
                    }]
                )
            )

            # Segment breakdown
            segments = pd.unique(segment_test)
            for seg in segments:
                if seg is None or (isinstance(seg, float) and np.isnan(seg)):
                    seg = "unknown"
                mask = segment_test == seg
                if mask.sum() < 2:
                    continue
                y_seg = y_test[mask]
                p_seg = p_pred[mask]
                ev_seg = _evaluate(y_seg, p_seg)
                wape_seg = _wape_like_classification(ev_seg["brier"], ev_seg["logloss"])
                all_metrics.append(
                    pd.DataFrame(
                        [{
                            "cutoff_month": cutoff_month,
                            "model_name": model_name,
                            "segment": str(seg),
                            "wape_like": wape_seg,
                            "auc": ev_seg["auc"],
                            "brier": ev_seg["brier"],
                            "logloss": ev_seg["logloss"],
                        }]
                    )
                )

    if not all_results:
        return

    results_df = pd.concat(all_results, ignore_index=True)
    metrics_df = pd.concat(all_metrics, ignore_index=True)

    # Ensure date columns are timezone-naive for DuckDB
    for col in ("cutoff_month", "renewal_month"):
        if col in results_df.columns:
            results_df[col] = pd.to_datetime(results_df[col]).dt.normalize()
    metrics_df["cutoff_month"] = pd.to_datetime(metrics_df["cutoff_month"]).dt.normalize()

    write_table(results_df, "ml_renewal_backtest_results", mode="replace", warehouse_dir=warehouse_dir)
    write_table(metrics_df, "ml_renewal_backtest_metrics", mode="replace", warehouse_dir=warehouse_dir)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Walk-forward backtest renewal ML models on last N renewal months.",
    )
    parser.add_argument(
        "--duckdb-path",
        type=str,
        default="./warehouse/revenue_forecasting.duckdb",
        help="Path to DuckDB database file",
    )
    parser.add_argument(
        "--model",
        type=str,
        choices=["logistic", "xgboost", "both"],
        default="both",
        help="Model(s) to backtest",
    )
    parser.add_argument(
        "--last-n-cutoffs",
        type=int,
        default=DEFAULT_LAST_N_CUTOFFS,
        help="Number of most recent renewal months to use as cutoffs (default: 6)",
    )
    args = parser.parse_args()

    warehouse_dir = _warehouse_dir_from_duckdb_path(args.duckdb_path)
    models = ["logistic", "xgboost"] if args.model == "both" else [args.model]

    run_backtest(
        warehouse_dir=warehouse_dir,
        last_n_cutoffs=args.last_n_cutoffs,
        models_to_run=models,
    )


if __name__ == "__main__":
    main()
