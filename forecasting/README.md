# ML calibration modules

**Purpose:** ML calibration for renewals and pipeline probabilities. Trains models (e.g. logistic regression, XGBoost), writes predictions into DuckDB tables, and supports walk-forward backtesting (WAPE, segment splits).

**Design:**

- Python modules in `forecasting/src/` read from the DuckDB warehouse (built by dbt), train models, and **write predictions back into DuckDB tables** consumed by dbt.
- dbt models that use ML predictions read these tables when present; otherwise they **fall back to deterministic rules**. The deterministic engine remains the always-working baseline.

**Model selection (production vs challenger):**

- **Logistic regression** is the default production model: stable, explainable, and used in forecast pipelines when ML is enabled.
- **XGBoost** is the challenger: train and backtest it to compare accuracy and tradeoffs; dbt uses logistic first when both are present.
- Preferred model per dataset can later be driven by config: `forecasting/config/model_selection.yml`.

**How to run:**

- From the repo root: `./scripts/ml_train_renewals.sh` (and similar scripts for pipeline probabilities, backtesting).
- Ensure `dbt seed` and `dbt run` have been executed so the warehouse is populated before training (or use sim mode: `./scripts/run_all.sh sim`).

**Data mode:** ML feature tables (`ml_features_renewals`, `ml_features_pipeline`) are built the same way whether input comes from **demo** seeds or **sim** Parquet; staging abstracts the source. **ML quality metrics** (backtest Brier, logloss, calibration) are most meaningful in **sim mode**, where scale and noise are controlled; demo seeds are small and can yield noisy or unreliable metrics.

**Layout:**

- `src/` — training, calibration, and DuckDB I/O.
- `config/` — model and backtest configuration (e.g. horizons, segments).
- `reports/` — backtest outputs and metrics (WAPE, segment splits).
