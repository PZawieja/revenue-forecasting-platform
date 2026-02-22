"""
Pipeline close-probability ML calibration: train Logistic Regression and XGBoost on ml_features_pipeline,
evaluate with time-based split by snapshot_month, write predictions and metrics to DuckDB.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, log_loss, roc_auc_score
from sklearn.preprocessing import OneHotEncoder, StandardScaler

try:
    import xgboost as xgb
except ImportError:
    xgb = None

from forecasting.src.io_duckdb import read_table, write_table

# Time-based split: last N snapshot months for validation
DEFAULT_VAL_MONTHS = 3

# Categorical columns to one-hot encode
CAT_COLS = ["segment", "stage", "opportunity_type"]

# Numeric feature columns
NUM_COLS = ["amount", "deal_age_months"]

TARGET = "closed_won_flag"
ID_COLS = ["company_id", "opportunity_id", "snapshot_month"]


def _warehouse_dir_from_duckdb_path(duckdb_path: str | Path) -> Path:
    return Path(duckdb_path).resolve().parent


def load_features(warehouse_dir: Optional[Path] = None) -> pd.DataFrame:
    """Load ml_features_pipeline from DuckDB."""
    sql = "SELECT * FROM main.ml_features_pipeline"
    df = read_table(sql, warehouse_dir=warehouse_dir)
    if df.empty:
        raise ValueError("ml_features_pipeline is empty; run dbt first.")
    return df


def prepare_features(
    df: pd.DataFrame,
    fit_encoder: Optional[OneHotEncoder] = None,
    fit_scaler: Optional[StandardScaler] = None,
    scale: bool = True,
) -> tuple:
    """One-hot encode categoricals and optionally scale numerics. Returns (X, enc, scaler)."""
    df = df.copy()
    for c in CAT_COLS:
        if c not in df.columns:
            df[c] = "__null__"
        else:
            df[c] = df[c].fillna("__null__").astype(str)
    for c in NUM_COLS:
        if c not in df.columns:
            df[c] = 0.0
        else:
            df[c] = df[c].fillna(0.0)

    if fit_encoder is None:
        enc = OneHotEncoder(handle_unknown="ignore", sparse_output=False)
        X_cat = enc.fit_transform(df[CAT_COLS])
    else:
        enc = fit_encoder
        X_cat = enc.transform(df[CAT_COLS])

    X_num = df[NUM_COLS].values
    if fit_scaler is None and scale:
        scaler = StandardScaler()
        X_num = scaler.fit_transform(X_num)
    elif fit_scaler is not None and scale:
        scaler = fit_scaler
        X_num = scaler.transform(X_num)
    else:
        scaler = fit_scaler

    X = np.hstack([X_cat, X_num])
    return X, enc, scaler


def time_split(
    df: pd.DataFrame,
    val_months: int = DEFAULT_VAL_MONTHS,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split by snapshot_month: train = older months, val = last val_months. No shuffle."""
    df = df.sort_values("snapshot_month").reset_index(drop=True)
    months = pd.Series(df["snapshot_month"].unique()).sort_values()
    if len(months) <= val_months:
        val_months = max(1, len(months))
    val_months_set = set(months.tail(val_months))
    train_df = df[~df["snapshot_month"].isin(val_months_set)]
    val_df = df[df["snapshot_month"].isin(val_months_set)]
    return train_df, val_df


def evaluate(y_true: np.ndarray, p_pred: np.ndarray) -> dict[str, float]:
    p_pred = np.clip(p_pred.astype(float), 1e-7, 1 - 1e-7)
    y_true = np.asarray(y_true, dtype=float)
    n_unique = len(np.unique(y_true))
    return {
        "auc": float(roc_auc_score(y_true, p_pred)) if n_unique > 1 else 0.0,
        "logloss": float(log_loss(y_true, p_pred)),
        "brier": float(np.mean((p_pred - y_true) ** 2)),
        "accuracy": float(accuracy_score(y_true, (p_pred >= 0.5).astype(int))),
    }


def train_logistic(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray,
    y_val: np.ndarray,
) -> tuple:
    model = LogisticRegression(max_iter=1000, random_state=42)
    model.fit(X_train, y_train)
    p_val = model.predict_proba(X_val)[:, 1]
    return model, evaluate(y_val, p_val)


def train_xgboost(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray,
    y_val: np.ndarray,
) -> tuple:
    if xgb is None:
        raise RuntimeError("xgboost is required; pip install xgboost")
    model = xgb.XGBClassifier(use_label_encoder=False, eval_metric="logloss", random_state=42)
    model.fit(X_train, y_train)
    p_val = model.predict_proba(X_val)[:, 1]
    return model, evaluate(y_val, p_val)


def run_pipeline(
    warehouse_dir: Optional[Path] = None,
    val_months: int = DEFAULT_VAL_MONTHS,
    models_to_train: Optional[list[str]] = None,
) -> None:
    if models_to_train is None:
        models_to_train = ["logistic", "xgboost"]

    df = load_features(warehouse_dir)
    train_df, val_df = time_split(df, val_months=val_months)

    X_train_scaled, enc, scaler = prepare_features(train_df, scale=True)
    y_train = train_df[TARGET].values
    X_val_scaled, _, _ = prepare_features(val_df, fit_encoder=enc, fit_scaler=scaler, scale=True)
    y_val = val_df[TARGET].values

    X_train_raw, enc_raw, _ = prepare_features(train_df, scale=False)
    X_val_raw, _, _ = prepare_features(val_df, fit_encoder=enc_raw, scale=False)

    as_of_month = df["snapshot_month"].max()
    if hasattr(as_of_month, "date"):
        as_of_month = as_of_month.date()
    elif isinstance(as_of_month, (np.datetime64, pd.Timestamp)):
        as_of_month = pd.Timestamp(as_of_month).date()
    created_at = datetime.now(timezone.utc)

    df_full = df.sort_values("snapshot_month").reset_index(drop=True)
    X_full_scaled, _, _ = prepare_features(df_full, scale=True)
    y_full = df_full[TARGET].values
    X_full_raw, _, _ = prepare_features(df_full, scale=False)

    all_predictions: list[pd.DataFrame] = []
    all_metrics: list[pd.DataFrame] = []

    for name in models_to_train:
        if name == "logistic":
            model, val_metrics = train_logistic(X_train_scaled, y_train, X_val_scaled, y_val)
            model_final = LogisticRegression(max_iter=1000, random_state=42)
            model_final.fit(X_full_scaled, y_full)
            p_pred = model_final.predict_proba(X_full_scaled)[:, 1]
        elif name == "xgboost":
            model, val_metrics = train_xgboost(X_train_raw, y_train, X_val_raw, y_val)
            model_final = xgb.XGBClassifier(use_label_encoder=False, eval_metric="logloss", random_state=42)
            model_final.fit(X_full_raw, y_full)
            p_pred = model_final.predict_proba(X_full_raw)[:, 1]
        else:
            continue

        pred_df = df_full[ID_COLS].copy()
        pred_df["as_of_month"] = as_of_month
        pred_df["model_name"] = name
        pred_df["p_close_ml"] = p_pred.astype(float)
        pred_df["created_at_utc"] = created_at
        all_predictions.append(pred_df)

        for metric_name, metric_value in val_metrics.items():
            all_metrics.append(
                pd.DataFrame(
                    [{
                        "as_of_month": as_of_month,
                        "model_name": name,
                        "dataset": "pipeline",
                        "metric_name": metric_name,
                        "metric_value": metric_value,
                    }]
                )
            )

    if not all_predictions:
        return

    predictions_df = pd.concat(all_predictions, ignore_index=True)
    metrics_df = pd.concat(all_metrics, ignore_index=True)

    write_table(predictions_df, "ml_pipeline_predictions", mode="replace", warehouse_dir=warehouse_dir)
    write_table(metrics_df, "ml_model_metrics", mode="replace", warehouse_dir=warehouse_dir)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Train pipeline close-probability models (logistic, xgboost) from ml_features_pipeline.",
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
        help="Model(s) to train",
    )
    parser.add_argument(
        "--val-months",
        type=int,
        default=DEFAULT_VAL_MONTHS,
        help="Number of most recent snapshot months for validation",
    )
    args = parser.parse_args()

    warehouse_dir = _warehouse_dir_from_duckdb_path(args.duckdb_path)
    models = ["logistic", "xgboost"] if args.model == "both" else [args.model]
    run_pipeline(warehouse_dir=warehouse_dir, val_months=args.val_months, models_to_train=models)


if __name__ == "__main__":
    main()
