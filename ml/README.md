# ML Extension (Renewal Probability)

**Note:** This folder is an **optional, alternative** ML workflow. The **primary** ML pipeline (renewals + pipeline, backtests, calibration, champion selection) lives in **`forecasting/`** and is run via **`./scripts/run_all.sh`** — see the repo [README](../README.md) and [Runbook](../docs/RUNBOOK.md). Use this `ml/` module when you need a standalone, per-company renewal model with Parquet-based predictions.

This folder contains a **portable, multi-company** ML module that integrates with the dbt + DuckDB revenue forecasting platform. It trains a **renewal probability model** (logistic regression + calibration) per `company_id` and writes predictions for dbt to consume.

## Prerequisites

1. **Build dbt ML datasets**  
   From the repo root (or `dbt/`):
   ```bash
   dbt seed && dbt run
   ```
   This builds `ml_features__renewal`, `ml_labels__renewal`, and `ml_dataset__renewal` in DuckDB. The ML models depend on these tables.

2. **Python environment**  
   Use the repo venv and install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
   Required: `pandas`, `pyarrow`, `scikit-learn`, `pyyaml`, and a DuckDB driver (e.g. `duckdb` for local DuckDB).

## Workflow

### 1. Generate training set

Export the dbt-built renewal dataset for a company to Parquet (deterministic sort for reproducibility):

```bash
python -m ml.src.build_training_set --company-id <company_id> [--output-path ml/outputs/training/renewal_dataset.parquet]
```

- Reads from the `ml_dataset__renewal` table in DuckDB (must exist after `dbt run`).
- Writes Parquet to `ml/outputs/training/` by default (gitignored).

### 2. Train renewal model

Train a baseline logistic regression with one-hot encoding and standardized numerics; split by time (earliest 80% train, latest 20% validation):

```bash
python -m ml.src.train_renewal_model --company-id <company_id> \
  [--input-path ml/outputs/training/renewal_dataset.parquet] \
  [--model-out ml/artifacts/renewal_model.joblib] \
  [--metrics-out ml/artifacts/renewal_metrics.json]
```

- Saves the fitted pipeline (preprocessor + classifier) as Joblib.
- Writes ROC AUC, PR AUC, Brier score, and calibration curve buckets to JSON.

### 3. Calibrate probabilities

Calibrate the model (e.g. isotonic regression) on the validation set to improve probability reliability:

```bash
python -m ml.src.calibrate_probabilities --company-id <company_id> \
  --model-in ml/artifacts/renewal_model.joblib \
  [--input-path ml/outputs/training/renewal_dataset.parquet] \
  [--model-out-calibrated ml/artifacts/renewal_model_calibrated.joblib]
```

- Reads the same training/validation split; fits calibration on validation.
- Saves a new pipeline that includes the calibrator.

### 4. Predict (scoring)

Score the latest available feature set (no labels) and write predictions for dbt:

```bash
python -m ml.src.predict_renewal --company-id <company_id> \
  [--model-in ml/artifacts/renewal_model_calibrated.joblib] \
  [--output-path ml/outputs/predictions/renewal_predictions.parquet]
```

- Reads from `ml_features__renewal` in DuckDB (latest snapshot).
- Output columns: `company_id`, `customer_id`, `renewal_month`, `as_of_month`, `p_renew_ml`.
- Writes Parquet to `ml/outputs/predictions/` (gitignored).

## Where artifacts and outputs go

| Path | Purpose | Gitignored |
|------|---------|------------|
| `ml/artifacts/` | Saved models (`.joblib`), metrics (`.json`) | Yes |
| `ml/outputs/` | Training Parquet, prediction Parquet | Yes |
| `ml/outputs/training/` | Exported `ml_dataset__renewal` per company | Yes |
| `ml/outputs/predictions/` | `renewal_predictions.parquet` for dbt | Yes |

Do not commit `ml/artifacts/` or `ml/outputs/`; they are in `.gitignore`.

## How dbt consumes predictions

The dbt model `ml_predictions__renewal` (under `dbt/models/intermediate/ml/`) reads the Parquet file at `../ml/outputs/predictions/renewal_predictions.parquet` (relative to the dbt project). DuckDB can query Parquet directly; the model uses `read_parquet()` or a mounted path so that downstream models can join ML probabilities (e.g. `p_renew_ml`) to the deterministic renewal forecast. Full integration (e.g. blending rule-based `p_renew` with `p_renew_ml`) is left as a TODO in the codebase.

## Multi-company

All scripts take `--company-id`. Train and predict per company; use the same codebase and config for every tenant. Model artifacts can be named by company (e.g. `renewal_model_<company_id>.joblib`) if you run multiple companies.

## Placeholders

- **Pipeline conversion model:** Not implemented; structure reserved in config and docs.
- **Expansion uplift model:** Not implemented; structure reserved in config and docs.
