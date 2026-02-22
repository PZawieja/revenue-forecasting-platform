# Revenue Forecasting Platform

A **Revenue Forecasting Platform** for B2B SaaS: an analytics engineering project that turns subscription, pipeline, and usage data into reproducible, scenario-based revenue forecasts—so finance and go-to-market teams can plan on a single source of truth.

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

## Future improvements

- **Python layer:** Scenario runner scripts (best/base/worst) and optional statistical or ML-based forecast components.
- **Documentation:** dbt docs generate + `docs/` content for architecture and scenario definitions.
- **CI:** GitHub Actions (or similar) for `dbt build` and tests on push/PR.
- **Data:** Optional connectors or pipelines for live CRM/usage sources; keep seeds for demos and tests.
- **Marts:** Formal forecast fact tables with scenario keys and versioning.

---

## Documentation

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
