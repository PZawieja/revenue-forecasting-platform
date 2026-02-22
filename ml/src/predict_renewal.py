"""
Score renewal probability from ml_features__renewal (no labels).
Outputs Parquet for dbt: company_id, customer_id, renewal_month, as_of_month, p_renew_ml.
"""

import argparse
from pathlib import Path

import joblib
import pandas as pd
import yaml

from ml.src.io_duckdb import read_sql
from ml.src.utils import get_repo_root


def load_config() -> dict:
    """Load model_config.yml from ml/config."""
    path = get_repo_root() / "ml" / "config" / "model_config.yml"
    with open(path) as f:
        return yaml.safe_load(f)


def main() -> None:
    parser = argparse.ArgumentParser(description="Predict renewal probability from features")
    parser.add_argument("--company-id", required=True, help="Filter features by company_id")
    parser.add_argument(
        "--model-in",
        type=Path,
        default=None,
        help="Model joblib path (default: ml/artifacts/renewal_model_calibrated.joblib)",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=None,
        help="Output Parquet (default: ml/outputs/predictions/renewal_predictions.parquet)",
    )
    args = parser.parse_args()

    root = get_repo_root()
    model_path = args.model_in or root / "ml" / "artifacts" / "renewal_model_calibrated.joblib"
    out_path = args.output_path or root / "ml" / "outputs" / "predictions" / "renewal_predictions.parquet"

    config = load_config()
    feature_cols = (
        config["renewal_model"]["categorical_features"]
        + config["renewal_model"]["numeric_features"]
    )

    model = joblib.load(model_path)
    company_escaped = args.company_id.replace("'", "''")
    query = f"""
        SELECT *
        FROM ml_features__renewal
        WHERE company_id = '{company_escaped}'
        ORDER BY company_id, customer_id, renewal_month
    """
    df = read_sql(query)
    if df.empty:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=["company_id", "customer_id", "renewal_month", "as_of_month", "p_renew_ml"]).to_parquet(
            out_path, index=False
        )
        print(f"No rows for company_id={args.company_id}; wrote empty Parquet to {out_path}")
        return

    X = df[feature_cols].copy()
    # Fill numeric NaNs with 0 so pipeline does not fail; categoricals handled by encoder
    for c in feature_cols:
        if c in X.columns and X[c].dtype in ("float64", "int64"):
            X[c] = X[c].fillna(0)
    proba = model.predict_proba(X)[:, 1]

    out = pd.DataFrame({
        "company_id": df["company_id"],
        "customer_id": df["customer_id"],
        "renewal_month": df["renewal_month"],
        "as_of_month": df["as_of_month"],
        "p_renew_ml": proba,
    })
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False)
    print(f"Wrote {len(out)} predictions to {out_path}")


if __name__ == "__main__":
    main()
