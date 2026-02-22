"""
Train baseline renewal probability model (logistic regression).
Time-based split; one-hot encode categoricals; standardize numerics.
Saves pipeline as joblib and metrics as JSON.
"""

import argparse
import json
from pathlib import Path

import pandas as pd
import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
import joblib
import yaml

from ml.src.evaluation import metrics_dict, ensure_serializable
from ml.src.utils import get_repo_root


def load_config() -> dict:
    """Load model_config.yml from ml/config."""
    path = get_repo_root() / "ml" / "config" / "model_config.yml"
    with open(path) as f:
        return yaml.safe_load(f)


def build_preprocessor(config: dict):
    """Build ColumnTransformer: one-hot for categoricals, standard scale for numerics."""
    cat_cols = config["renewal_model"]["categorical_features"]
    num_cols = config["renewal_model"]["numeric_features"]
    return ColumnTransformer(
        [
            ("cat", OneHotEncoder(drop="first", sparse_output=False, handle_unknown="ignore"), cat_cols),
            ("num", StandardScaler(), num_cols),
        ],
        remainder="drop",
    )


def time_split(
    df: pd.DataFrame,
    renewal_month_col: str,
    train_pct: float,
    random_state: int,
) -> tuple:
    """Split by earliest train_pct of renewal_months (train) vs latest (validation)."""
    months = df[renewal_month_col].dropna().unique()
    months = sorted(months)
    n = len(months)
    if n == 0:
        raise ValueError("No renewal months in dataset")
    cut = max(1, int(n * train_pct))
    train_months = set(months[:cut])
    val_months = set(months[cut:])
    train_idx = df[renewal_month_col].isin(train_months)
    val_idx = df[renewal_month_col].isin(val_months)
    return df[train_idx], df[val_idx]


def main() -> None:
    parser = argparse.ArgumentParser(description="Train renewal logistic regression model")
    parser.add_argument("--company-id", required=True, help="Company identifier (for config/logging)")
    parser.add_argument(
        "--input-path",
        type=Path,
        default=None,
        help="Input Parquet (default: ml/outputs/training/renewal_dataset.parquet)",
    )
    parser.add_argument(
        "--model-out",
        type=Path,
        default=None,
        help="Output joblib path (default: ml/artifacts/renewal_model.joblib)",
    )
    parser.add_argument(
        "--metrics-out",
        type=Path,
        default=None,
        help="Output metrics JSON (default: ml/artifacts/renewal_metrics.json)",
    )
    args = parser.parse_args()

    root = get_repo_root()
    input_path = args.input_path or root / "ml" / "outputs" / "training" / "renewal_dataset.parquet"
    model_out = args.model_out or root / "ml" / "artifacts" / "renewal_model.joblib"
    metrics_out = args.metrics_out or root / "ml" / "artifacts" / "renewal_metrics.json"

    config = load_config()
    rm = config["renewal_model"]
    target = rm["target_column"]
    cat_cols = rm["categorical_features"]
    num_cols = rm["numeric_features"]
    train_pct = rm["time_split"]["train_pct"]
    random_state = rm["random_state"]

    df = pd.read_parquet(input_path)
    if df.empty:
        raise ValueError(f"Empty dataset at {input_path}")

    feature_cols = [c for c in cat_cols + num_cols if c in df.columns]
    missing = set(cat_cols + num_cols) - set(df.columns)
    if missing:
        raise ValueError(f"Missing feature columns: {missing}")

    df = df.dropna(subset=feature_cols + [target, "renewal_month"])
    X = df[feature_cols]
    y = df[target].astype(int)

    train_df, val_df = time_split(df, "renewal_month", train_pct, random_state)
    X_train = train_df[feature_cols]
    y_train = train_df[target].astype(int)
    X_val = val_df[feature_cols]
    y_val = val_df[target].astype(int)

    preprocessor = build_preprocessor(config)
    clf = LogisticRegression(random_state=random_state, max_iter=1000)
    pipe = Pipeline([("preprocessor", preprocessor), ("classifier", clf)])
    pipe.fit(X_train, y_train)

    y_prob_val = pipe.predict_proba(X_val)[:, 1]
    metrics = metrics_dict(y_val.values, y_prob_val)
    metrics["company_id"] = args.company_id
    metrics["n_train"] = int(len(train_df))
    metrics["n_val"] = int(len(val_df))

    model_out.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipe, model_out)
    metrics_out.parent.mkdir(parents=True, exist_ok=True)
    with open(metrics_out, "w") as f:
        json.dump(ensure_serializable(metrics), f, indent=2)

    print(f"Saved model to {model_out}")
    print(f"Saved metrics to {metrics_out}")
    print(f"ROC AUC: {metrics['roc_auc']:.4f}  PR AUC: {metrics['pr_auc']:.4f}  Brier: {metrics['brier_score']:.4f}")


if __name__ == "__main__":
    main()
