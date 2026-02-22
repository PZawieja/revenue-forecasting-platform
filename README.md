# Revenue Forecasting Platform

A **Revenue Forecasting Platform** for B2B SaaS: an analytics engineering project that turns subscription, pipeline, and usage data into reproducible, scenario-based revenue forecasts—so finance and go-to-market teams can plan on a single source of truth.

---

## Start here

New to the repo? Get up to speed in a few minutes:

| Doc | Purpose |
|-----|---------|
| **[Architecture overview](docs/architecture_overview.md)** | System narrative, architecture diagram (Mermaid), and design decisions. |
| **[Demo script (3 min)](docs/demo_script_3min.md)** | Executive-grade walkthrough: problem → Forecast → ARR Waterfall → Risk Radar → Model Intelligence → product angle. |
| **[Founder pitch one-pager](docs/founder_pitch_onepager.md)** | Product name, ICP, value, differentiators, MVP vs v2, and high-level pricing tiers. |

---

## Executive summary

**Problem:** Revenue plans are often scattered across spreadsheets and CRM snapshots, with no clear lineage or way to run what-if scenarios (e.g. churn, expansion, pipeline conversion).

**Solution:** This project provides a dbt-centric pipeline that ingests customers, products, subscriptions, pipeline snapshots, and usage into a DuckDB warehouse, then builds staging → intermediate → marts models. The design supports **scenario-based forecasting** (e.g. best/base/worst cases) and keeps logic version-controlled, testable, and documented—suitable for portfolio or production use.

---

## Tech stack

| Layer | Technology |
|-------|------------|
| Transform & orchestration | **dbt** (models, seeds, tests, docs) |
| Analytical database | **DuckDB** (local file-based; `dbt-duckdb` adapter) |
| Transformation logic | **SQL** (staging, intermediate, marts) |
| Future | **Python** (scenario scripts, optional ML/forecasting) |
| Forecasting approach | **Scenario-based** (assumptions-driven revenue scenarios) |

---

## Architecture overview

- **Seeds / raw:** CSV seeds (or future ingestion) for customers, products, subscriptions, pipeline snapshots, usage.
- **Staging:** Source-aligned models; light cleaning and renaming.
- **Intermediate:** Business logic: contract rollups, cohort flags, pipeline stage logic, usage aggregates.
- **Marts:** Reporting-ready datasets: revenue by segment, renewal/expansion/churn views, and forecast inputs.
- **Warehouse:** Local DuckDB file in `warehouse/` (only `*.duckdb` is gitignored; the folder is committed).
- **Scripts:** Repo-root helpers for venv setup, `dbt debug`, `dbt seed`, `dbt run` (profile and paths handled inside scripts).
- **Docs:** Project and architecture documentation live in `docs/`.

---

## Roadmap

| Phase | Focus |
|-------|--------|
| **1** | Staging models from seeds; data dictionary and tests. |
| **2** | Intermediate models: subscription rollups, pipeline stage movement, usage trends. |
| **3** | Marts: revenue views, renewal/expansion/churn, forecast input tables. |
| **4** | Scenario-based forecasting (assumption tables + SQL/Python); docs and CI. |

---

## Quickstart

From the repo root, using the provided scripts (they use **`DBT_PROFILES_DIR=./profiles`** from within `dbt/` so no `~/.dbt` config is needed):

```bash
./scripts/setup.sh       # one-time: create .venv, install requirements.txt
./scripts/dbt_debug.sh   # verify connection
./scripts/dbt_seed.sh    # load seeds
./scripts/dbt_run.sh     # build models
./scripts/dbt_test.sh    # run tests
```

Profile is in `dbt/profiles/profiles.yml` (DuckDB at `../warehouse/revenue_forecasting.duckdb` when run from `dbt/`).

### Local demo (Streamlit)

From the repo root, run the Revenue Intelligence Executive Cockpit (reads from DuckDB + dbt marts):

```bash
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app/Home.py
```

If the DuckDB file or marts are missing, the app shows a **Run checklist** with the exact commands to build tables and optionally train ML.

### How to run it locally (after Prompt 42+)

From repo root:

```bash
source .venv/bin/activate
pip install -r requirements.txt

# Build tables
export DBT_PROFILES_DIR=./dbt/profiles
cd dbt
dbt seed --full-refresh
dbt run
dbt test
cd ..

# (Optional) Train ML and repopulate
./scripts/run_all.sh

# Launch demo
streamlit run app/Home.py
```

### One-command demo

From repo root:

```bash
make setup
make demo
```

`make demo` runs a full build (dbt → ML → dbt) then starts the Streamlit app. For build-only: `make build`. To run only the app: `make app`.

### Export artifacts

Export key marts and ML outputs as CSVs into `docs/artifacts/` for sharing without running Streamlit (e.g. for docs or handoffs). Generated `*.csv` files are gitignored; the folder is kept via `docs/artifacts/.gitkeep`.

```bash
make build
./scripts/export_demo_artifacts.sh
```

Exports: executive forecast summary (latest 12 months), ARR waterfall (latest 6 months), churn risk watchlist (top 20), backtest metrics, `ml_model_selection`, and calibration bins for preferred models.

---

## Run types

### Baseline run (rules only)

Forecast using **rule-based** renewal and pipeline stage probabilities only (no ML). Runs dbt so forecasts use config-driven probabilities.

```bash
./scripts/setup.sh
./scripts/dbt_seed.sh
./scripts/dbt_run.sh
```

### ML calibrated run (preferred model selection)

Train renewal and pipeline models, **publish the preferred model per dataset** to DuckDB (`ml_model_selection`), then rerun dbt. The forecast uses the **preferred ML model** (logistic or xgboost) per dataset, with rules as fallback when ML is missing.

```bash
./scripts/setup.sh
./scripts/run_all.sh
```

`run_all.sh` runs dbt (seed + run), **publish_model_selection** (reads `forecasting/config/model_selection.yml` → writes `ml_model_selection`), trains both models, then reruns dbt so staging filters to the preferred model and the forecast consumes it.

Single-domain ML:

- `./scripts/ml_train_renewals.sh` — dbt, train renewals, rerun dbt.
- `./scripts/ml_train_pipeline.sh` — dbt, train pipeline, rerun dbt.

**Switching preferred model:** Edit `forecasting/config/model_selection.yml` (set `preferred_model: logistic` or `preferred_model: xgboost` per dataset), then run `./scripts/publish_model_selection.sh` and rerun dbt (e.g. `./scripts/dbt_run.sh`). No retraining needed; dbt will use the newly preferred model from existing predictions.

All scripts ensure `.venv` exists (prompt to run `./scripts/setup.sh` if not), create `./warehouse/` if needed, and run dbt with `DBT_PROFILES_DIR=./profiles` from `dbt/`.

### Backtesting ML

Walk-forward backtests for renewal and pipeline models (no dbt rerun after). Results go to `ml_*_backtest_results` and `ml_*_backtest_metrics` in DuckDB. After backtests, run calibration reports to populate `ml_calibration_bins`, `ml_threshold_metrics`, and `ml_cost_curves` (exposed as marts for exploration).

```bash
./scripts/ml_backtest_renewals.sh
./scripts/ml_backtest_pipeline.sh
./scripts/ml_calibration_reports.sh
```

`run_all.sh` also runs backtests, calibration reports, **champion/challenger selection**, and a final dbt run so forecasts use the chosen model.

### Champion/Challenger ML selection

Preferred model per dataset (renewals, pipeline) can be chosen automatically from backtest performance. The script `forecasting/src/select_champion_model.py` reads `ml_renewal_backtest_metrics` and `ml_pipeline_backtest_metrics`, uses the **latest 6 cutoff months** per model, and computes a composite score from **mean and standard deviation of logloss and Brier** (lower is better). The model with the **lowest score** becomes the champion. A **stability guardrail** applies: if the best model leads by less than 1% on that score, **logistic** is chosen so the simpler, more stable model is preferred when the two are close. The chosen model is written to **ml_model_selection** (with `selection_reason`, `score_logistic`, `score_xgboost`) so dbt staging and forecasts use it as the source of truth. You can still override via `publish_model_selection` (YAML) if you want a fixed choice instead of data-driven selection.

---

## ML model quality

The platform reports portfolio-grade calibration and business-impact metrics for renewal and pipeline probability models (from backtest outputs):

- **Brier score** — Mean squared error between predicted probabilities and outcomes (0 = perfect, 0.25 = no better than 50/50). Reported in `ml_renewal_backtest_metrics` and `ml_pipeline_backtest_metrics` per cutoff and model; lower is better.
- **Calibration bins** — Predictions are grouped into 10 probability bins (e.g. 0–0.1, 0.1–0.2, …). For each bin we store the average predicted probability (`p_pred_mean`) and the empirical success rate (`y_true_rate`). A well-calibrated model has `p_pred_mean ≈ y_true_rate` in every bin; large gaps indicate over- or under-confidence. Exposed in **mart_ml_calibration_bins**.
- **Threshold metrics** — For thresholds 0.1–0.9 we compute precision, recall, FPR, FNR, and confusion counts (tp, fp, tn, fn) so you can tune decision thresholds for operations (e.g. who gets CSM outreach). Exposed in **mart_ml_threshold_metrics**.
- **Threshold cost curves** — For each threshold we apply dataset-specific cost assumptions (e.g. renewals: cost of a missed at-risk renewal vs unnecessary outreach; pipeline: cost of forecast over- vs under-statement) and write **expected_cost = fn×fn_cost + fp×fp_cost**. Exposed in **mart_ml_cost_curves** so you can pick the threshold that minimizes expected cost.

Run `./scripts/ml_calibration_reports.sh` after backtests to refresh these tables; then use the marts in dbt docs or a BI tool.

---

## How to explore the outputs

After `dbt run`, the main **marts** to explore (e.g. in a BI tool or `dbt docs serve`) and what each answers:

| Mart | What it answers |
|------|------------------|
| **mart_executive_forecast_summary** | High-level forecast and scenario summary for execs. |
| **fct_revenue_forecast_with_intervals** | Revenue forecast by month with best/base/worst (or confidence) intervals. |
| **mart_arr_waterfall_monthly** | Month-over-month ARR movement: new, expansion, churn, net. |
| **mart_arr_bridge_customer_monthly** | ARR change by customer (who drove growth or churn). |
| **mart_churn_risk_watchlist** | Which customers are at risk and their renewal probabilities. |
| **mart_bookings_vs_revenue_monthly** | Bookings vs recognized revenue for reconciliation. |
| **mart_forecast_coverage_metrics** | How much of ARR/forecast is covered by models vs assumptions. |
| **mart_forecast_explainability_monthly** | Why the forecast changed (renewals, new biz, expansion, etc.). |

Exposures in `dbt/models/exposures.yml` (Executive Forecast Summary, ARR Waterfall, Churn Risk Watchlist) map these marts to downstream dashboards for lineage in `dbt docs`.

---

## Reproducibility & local setup

**Requirements:** Python 3.9+, Git.

1. **Clone and enter the repo**
   ```bash
   git clone <repo-url> && cd revenue-forecasting-platform
   ```

2. **One-time setup (venv + dependencies)**
   ```bash
   ./scripts/setup.sh
   ```
   This creates `.venv`, activates it, and runs `pip install -r requirements.txt`.

3. **Configure the dbt profile**
   - A **repo-local profile** lives in `dbt/profiles/profiles.yml`. The scripts set **`DBT_PROFILES_DIR=./profiles`** when running from `dbt/`, so no `~/.dbt` config is required.
   - The DuckDB path is `../warehouse/revenue_forecasting.duckdb` (relative to `dbt/`). The `warehouse/` directory is committed; only `*.duckdb` files are ignored.

4. **Run dbt** — use the Quickstart scripts above, or manually from repo root:
   ```bash
   export DBT_PROFILES_DIR="$(pwd)/dbt/profiles"
   cd dbt && ../.venv/bin/dbt debug
   ../.venv/bin/dbt seed && ../.venv/bin/dbt run && ../.venv/bin/dbt test
   ```
   From inside `dbt/`: `export DBT_PROFILES_DIR=./profiles` then `../.venv/bin/dbt seed` (etc.).

---

## CI

A GitHub Actions workflow (`.github/workflows/dbt_ci.yml`) runs on every **push to `main`** and on every **pull request** targeting `main`. It uses `ubuntu-latest` and Python 3.11, installs dependencies from `requirements.txt`, creates the `warehouse/` directory (DuckDB is created at `warehouse/revenue_forecasting.duckdb` at runtime), and runs dbt from the `dbt/` directory with `DBT_PROFILES_DIR=./profiles`. **Job 1 (dbt):** `dbt --version`, `dbt debug`, `dbt seed --full-refresh`, `dbt run`, `dbt test`. No secrets or external services; all paths are relative and repo-local.

### CI ML checks

An optional second job **ml_ci** runs after the dbt job succeeds. It re-runs dbt (seed + run) to build ML feature tables, then runs the full ML pipeline from the repo root:

1. **Publish model selection** — Writes `ml_model_selection` from `forecasting/config/model_selection.yml`.
2. **Train both datasets** — `train_renewals` and `train_pipeline` with `--model both`.
3. **Backtests** — `backtest_renewals` and `backtest_pipeline` with `--model both`.
4. **Calibration reports** — Builds `ml_calibration_bins`, `ml_threshold_metrics`, `ml_cost_curves`.
5. **ML quality gates** — `ci_quality_gates` checks backtest metrics against thresholds; see below.
6. **dbt run + dbt test** — So marts that read ML tables compile and tests pass.

**Quality gates (illustrative, tunable):** The script `forecasting/src/ci_quality_gates.py` reads `ml_renewal_backtest_metrics` and `ml_pipeline_backtest_metrics`, takes the latest cutoff per model, and enforces:

- **Renewals:** Brier ≤ 0.25, logloss ≤ 0.75.
- **Pipeline:** Brier ≤ 0.30, logloss ≤ 0.80.

If **both** logistic and xgboost exceed the thresholds for a given dataset, the job fails (exit 1). If only one model fails, the job passes but prints a warning so you can tune. These defaults are mild for synthetic/CI; for real data, relax or tighten thresholds via CLI args (`--renewals-brier-max`, `--pipeline-logloss-max`, etc.) or change the script defaults.

---

## Future improvements

- **Python layer:** Scenario runner scripts (best/base/worst) and optional statistical or ML-based forecast components.
- **Documentation:** dbt docs generate + `docs/` content for architecture and scenario definitions.
- **Data:** Optional connectors or pipelines for live CRM/usage sources; keep seeds for demos and tests.
- **Marts:** Formal forecast fact tables with scenario keys and versioning.

---

## Screenshots

| Page | Placeholder |
|------|-------------|
| **Forecast vs Actual** | *Screenshot: forecast bands, scenarios, confidence.* |
| **ARR Waterfall** | *Screenshot: bridge and reconciliation.* |
| **Risk Radar** | *Screenshot: churn risk watchlist and top ARR movers.* |
| **Model Intelligence** | *Screenshot: champion selection and calibration.* |

**Note:** Run Streamlit locally (`streamlit run app/Home.py`) and take screenshots; add them under `docs/screenshots/` and link here if desired.

---

## Documentation

- **[Architecture overview](docs/architecture_overview.md)** — System narrative, Mermaid diagram, design decisions.
- **[Demo script (3 min)](docs/demo_script_3min.md)** — Executive demo walkthrough.
- **[Founder pitch one-pager](docs/founder_pitch_onepager.md)** — Product pitch and MVP/v2 scope.
- **[Company Onboarding Guide](docs/company_onboarding_guide.md)** — How to map common source systems (Salesforce, HubSpot, Stripe, Chargebee) into the platform’s canonical data contract; required and optional tables, mapping patterns, pitfalls, minimum viable onboarding checklist, and data quality controls.
- `docs/` — Additional project and architecture docs (metrics, configuration, data contract).

---

## ML Extension (Optional)

An optional **ML module** (`ml/`) provides a **renewal probability model** (logistic regression + calibration) that is portable across companies and integrates with dbt + DuckDB:

- **dbt** builds time-valid feature and label tables (`ml_features__renewal`, `ml_labels__renewal`, `ml_dataset__renewal`) so training data never leaks future information.
- **Python scripts** (run from repo root) train a model per `company_id`, calibrate probabilities, and write predictions to Parquet; **dbt** can then read that Parquet via `ml_predictions__renewal` for downstream use.
- The same codebase and config support multiple companies; artifacts and outputs live in gitignored `ml/artifacts/` and `ml/outputs/`.

**Workflow:** After `dbt seed` and `dbt run`, run `build_training_set` → `train_renewal_model` → `calibrate_probabilities` → `predict_renewal` (see `ml/README.md`). The deterministic rule-based forecast in `int_renewal_probabilities` remains the default; the ML output is available for blending or comparison (TODO: full integration).

---

## Repository layout

- `dbt/` — dbt project (models, seeds, macros, tests, profiles).
- `ml/` — Optional ML module (renewal model, config, scripts, artifacts, outputs).
- `scripts/` — Setup and dbt runner scripts.
- `warehouse/` — Local DuckDB database directory (empty in repo; `*.duckdb` ignored).
- `docs/` — Project and architecture documentation.
