"""
Renewal ML calibration pipeline: train Logistic Regression and XGBoost on ml_features_renewals,
evaluate with time-based split, write predictions and metrics to DuckDB.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.dummy import DummyClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, log_loss
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from forecasting.src.io_duckdb import read_table, write_table

# Default validation window: last N renewal months
DEFAULT_VAL_MONTHS = 3

# Categorical columns to one-hot encode
CAT_COLS = ["segment", "segment_group", "slope_bucket"]

# Numeric feature columns (excluding target and ids)
NUM_COLS = [
    "current_mrr_pre_renewal",
    "months_to_renewal",
    "health_score_1_10",
    "trailing_3m_usage_per_user",
    "tenure_months",
]

TARGET = "renewed_flag"
ID_COLS = ["company_id", "customer_id", "renewal_month"]


def _warehouse_dir_from_duckdb_path(duckdb_path: str | Path) -> Path:
    """Derive warehouse directory from full DuckDB file path."""
    return Path(duckdb_path).resolve().parent


def load_features(warehouse_dir: Optional[Path] = None) -> pd.DataFrame:
    """Load ml_features_renewals from DuckDB."""
    sql = "SELECT * FROM main.ml_features_renewals"
    df = read_table(sql, warehouse_dir=warehouse_dir)
    if df.empty:
        raise ValueError("ml_features_renewals is empty; run dbt first.")
    return df


def prepare_features(
    df: pd.DataFrame,
    fit_encoder: Optional[OneHotEncoder] = None,
    fit_scaler: Optional[StandardScaler] = None,
    scale: bool = True,
):
    """
    One-hot encode categoricals and optionally scale numerics.
    Returns (X, encoder, scaler) for train, or (X, encoder, scaler) for predict using fit_encoder/fit_scaler.
    """
    df = df.copy()
    # Ensure all feature columns exist; fill missing
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

    # One-hot encode (always same column order for fit/transform)
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
    """Split by renewal_month: train = older months, val = last val_months. No shuffle."""
    df = df.sort_values("renewal_month").reset_index(drop=True)
    months = df["renewal_month"].unique()
    if len(months) <= val_months:
        # Too few months: use last month as val
        val_months = 1
    val_months_set = set(pd.Series(months).tail(val_months))
    train_df = df[~df["renewal_month"].isin(val_months_set)]
    val_df = df[df["renewal_month"].isin(val_months_set)]
    return train_df, val_df


def brier_score(y_true: np.ndarray, p_pred: np.ndarray) -> float:
    """Brier score = mean((p - y)^2)."""
    return float(np.mean((p_pred - y_true) ** 2))


def evaluate(y_true: np.ndarray, p_pred: np.ndarray) -> dict[str, float]:
    """Compute auc, logloss, brier, accuracy. p_pred = probability of class 1."""
    from sklearn.metrics import roc_auc_score

    p_pred = np.clip(p_pred, 1e-7, 1 - 1e-7)
    logloss = float(log_loss(y_true, p_pred, labels=[0.0, 1.0]))
    return {
        "auc": float(roc_auc_score(y_true, p_pred)) if len(np.unique(y_true)) > 1 else 0.0,
        "logloss": logloss,
        "brier": brier_score(y_true, p_pred),
        "accuracy": float(accuracy_score(y_true, (p_pred >= 0.5).astype(int))),
    }


def _proba_positive(model, X: np.ndarray) -> np.ndarray:
    """Probability of positive class (index 1); handles DummyClassifier with single class."""
    proba = model.predict_proba(X)
    if proba.shape[1] > 1:
        return proba[:, 1]
    return (1.0 - proba[:, 0]) if model.classes_[0] == 0 else proba[:, 0]


def train_logistic(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray,
    y_val: np.ndarray,
) -> tuple:
    """Train Logistic Regression (scaled features), return model and validation metrics."""
    try:
        model = LogisticRegression(max_iter=1000, random_state=42)
        model.fit(X_train, y_train)
    except ValueError:
        model = DummyClassifier(strategy="most_frequent", random_state=42)
        model.fit(X_train, y_train)
    p_val = _proba_positive(model, X_val)
    metrics = evaluate(y_val, p_val)
    return model, metrics


def train_xgboost(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray,
    y_val: np.ndarray,
) -> tuple:
    """Train XGBoost classifier, return model and validation metrics."""
    try:
        import xgboost as xgb
    except Exception as e:
        raise RuntimeError("xgboost is required and could not be loaded; install with pip install xgboost. On macOS you may need: brew install libomp") from e
    model = xgb.XGBClassifier(
        use_label_encoder=False,
        eval_metric="logloss",
        random_state=42,
    )
    model.fit(X_train, y_train)
    p_val = model.predict_proba(X_val)[:, 1]
    metrics = evaluate(y_val, p_val)
    return model, metrics


def run_pipeline(
    warehouse_dir: Optional[Path] = None,
    val_months: int = DEFAULT_VAL_MONTHS,
    models_to_train: list[str] | None = None,
) -> None:
    """
    Load features, time-split, train selected models, write ml_renewal_predictions and ml_model_metrics.
    models_to_train: ['logistic', 'xgboost'] or subset.
    """
    if models_to_train is None:
        models_to_train = ["logistic", "xgboost"]

    df = load_features(warehouse_dir)
    train_df, val_df = time_split(df, val_months=val_months)

    # Prepare train/val feature matrices (with scaling for logistic)
    X_train_scaled, enc, scaler = prepare_features(train_df, scale=True)
    y_train = train_df[TARGET].values
    X_val_scaled, _, _ = prepare_features(val_df, fit_encoder=enc, fit_scaler=scaler, scale=True)
    y_val = val_df[TARGET].values

    # Unscaled for XGBoost (optional; we use same encoded features, no scale for xgb)
    X_train_raw, enc_raw, _ = prepare_features(train_df, scale=False)
    X_val_raw, _, _ = prepare_features(val_df, fit_encoder=enc_raw, scale=False)

    # as_of_month: run month default = max renewal_month in features
    as_of_month = df["renewal_month"].max()
    if hasattr(as_of_month, "date"):
        as_of_month = as_of_month.date()
    elif isinstance(as_of_month, (np.datetime64, pd.Timestamp)):
        as_of_month = pd.Timestamp(as_of_month).date()
    created_at = datetime.now(timezone.utc)

    all_predictions: list[pd.DataFrame] = []
    all_metrics: list[pd.DataFrame] = []

    # Refit on full data for final predictions (train + val)
    df_full = df.sort_values("renewal_month").reset_index(drop=True)
    X_full_scaled, enc_final, scaler_final = prepare_features(df_full, scale=True)
    y_full = df_full[TARGET].values
    X_full_raw, enc_final_raw, _ = prepare_features(df_full, scale=False)

    for name in models_to_train:
        if name == "logistic":
            model, val_metrics = train_logistic(X_train_scaled, y_train, X_val_scaled, y_val)
            try:
                model_final = LogisticRegression(max_iter=1000, random_state=42)
                model_final.fit(X_full_scaled, y_full)
            except ValueError:
                model_final = DummyClassifier(strategy="most_frequent", random_state=42)
                model_final.fit(X_full_scaled, y_full)
            p_pred = _proba_positive(model_final, X_full_scaled)
        elif name == "xgboost":
            import xgboost as xgb
            model, val_metrics = train_xgboost(X_train_raw, y_train, X_val_raw, y_val)
            model_final = xgb.XGBClassifier(use_label_encoder=False, eval_metric="logloss", random_state=42)
            model_final.fit(X_full_raw, y_full)
            p_pred = model_final.predict_proba(X_full_raw)[:, 1]
        else:
            continue

        # Predictions table: one row per (company_id, customer_id, renewal_month) per model
        pred_df = df_full[ID_COLS].copy()
        pred_df["as_of_month"] = as_of_month
        pred_df["model_name"] = name
        pred_df["p_renew_ml"] = p_pred.astype(float)
        pred_df["created_at_utc"] = created_at
        all_predictions.append(pred_df)

        # Metrics table
        for metric_name, metric_value in val_metrics.items():
            all_metrics.append(
                pd.DataFrame(
                    [{
                        "as_of_month": as_of_month,
                        "model_name": name,
                        "dataset": "renewals",
                        "metric_name": metric_name,
                        "metric_value": metric_value,
                    }]
                )
            )

    if not all_predictions:
        return

    predictions_df = pd.concat(all_predictions, ignore_index=True)
    metrics_df = pd.concat(all_metrics, ignore_index=True)

    write_table(predictions_df, "ml_renewal_predictions", mode="replace", warehouse_dir=warehouse_dir)
    write_table(metrics_df, "ml_model_metrics", mode="replace", warehouse_dir=warehouse_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description="Train renewal ML models (logistic, xgboost) from ml_features_renewals.")
    parser.add_argument(
        "--duckdb-path",
        type=str,
        default="./warehouse/revenue_forecasting.duckdb",
        help="Path to DuckDB database file (default: ./warehouse/revenue_forecasting.duckdb)",
    )
    parser.add_argument(
        "--model",
        type=str,
        choices=["logistic", "xgboost", "both"],
        default="both",
        help="Model(s) to train: logistic, xgboost, or both",
    )
    parser.add_argument(
        "--val-months",
        type=int,
        default=DEFAULT_VAL_MONTHS,
        help="Number of most recent renewal months to use for validation (default: 3)",
    )
    args = parser.parse_args()

    warehouse_dir = _warehouse_dir_from_duckdb_path(args.duckdb_path)
    models = ["logistic", "xgboost"] if args.model == "both" else [args.model]

    run_pipeline(warehouse_dir=warehouse_dir, val_months=args.val_months, models_to_train=models)


if __name__ == "__main__":
    main()
