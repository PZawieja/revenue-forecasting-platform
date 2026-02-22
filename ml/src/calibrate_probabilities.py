"""
Calibrate renewal model probabilities using isotonic regression on validation set.
Saves a new pipeline (preprocessor + classifier + calibrator) as joblib.
"""

import argparse
from pathlib import Path

import pandas as pd
import joblib
from sklearn.calibration import CalibratedClassifierCV
import yaml

from ml.src.utils import get_repo_root


def load_config() -> dict:
    """Load model_config.yml from ml/config."""
    path = get_repo_root() / "ml" / "config" / "model_config.yml"
    with open(path) as f:
        return yaml.safe_load(f)


def main() -> None:
    parser = argparse.ArgumentParser(description="Calibrate renewal model on validation set")
    parser.add_argument("--company-id", required=True, help="Company identifier")
    parser.add_argument("--model-in", required=True, type=Path, help="Fitted model joblib path")
    parser.add_argument(
        "--input-path",
        type=Path,
        default=None,
        help="Training/validation Parquet (default: ml/outputs/training/renewal_dataset.parquet)",
    )
    parser.add_argument(
        "--model-out-calibrated",
        type=Path,
        default=None,
        help="Output calibrated model path (default: ml/artifacts/renewal_model_calibrated.joblib)",
    )
    args = parser.parse_args()

    root = get_repo_root()
    config = load_config()
    train_pct = config["renewal_model"]["time_split"]["train_pct"]
    target = config["renewal_model"]["target_column"]
    feature_cols = (
        config["renewal_model"]["categorical_features"]
        + config["renewal_model"]["numeric_features"]
    )

    input_path = args.input_path or root / "ml" / "outputs" / "training" / "renewal_dataset.parquet"
    model_out = args.model_out_calibrated or root / "ml" / "artifacts" / "renewal_model_calibrated.joblib"

    pipe = joblib.load(args.model_in)
    df = pd.read_parquet(input_path)
    df = df.dropna(subset=feature_cols + [target, "renewal_month"])

    # Same time split as training: use latest 20% as validation for calibration
    months = sorted(df["renewal_month"].dropna().unique())
    n = len(months)
    cut = max(1, int(n * train_pct))
    val_months = set(months[cut:])
    val_df = df[df["renewal_month"].isin(val_months)]
    X_val = val_df[feature_cols]
    y_val = val_df[target].astype(int)

    # Wrap the existing pipeline in CalibratedClassifierCV (isotonic)
    calibrated = CalibratedClassifierCV(pipe, method="isotonic", cv="prefit")
    calibrated.fit(X_val, y_val)

    model_out.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(calibrated, model_out)
    print(f"Saved calibrated model to {model_out}")


if __name__ == "__main__":
    main()
