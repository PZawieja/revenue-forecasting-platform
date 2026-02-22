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
   - Copy `dbt/profiles/profiles.yml.example` to `dbt/profiles/profiles.yml`, **or**
   - Copy it to `~/.dbt/profiles.yml` if you prefer a user-level profile.
   - The DuckDB path in the example points to `warehouse/revenue_forecasting.duckdb` (relative to the project when running from `dbt/`). Create the `warehouse/` directory if it does not exist; it is committed (only `*.duckdb` files are ignored).

4. **Run dbt (from repo root, scripts set the profile path)**
   ```bash
   ./scripts/dbt_debug.sh    # connection check
   ./scripts/dbt_seed.sh     # load seeds
   ./scripts/dbt_run.sh      # build models
   ```
   Or manually, from repo root:
   ```bash
   export DBT_PROFILES_DIR="$(pwd)/dbt/profiles"
   cd dbt && ../.venv/bin/dbt debug
   cd dbt && ../.venv/bin/dbt seed && ../.venv/bin/dbt run
   ```

5. **Run tests**
   ```bash
   cd dbt && ../.venv/bin/dbt test
   ```
   (With `DBT_PROFILES_DIR` set as above if not using the scripts.)

---

## Future improvements

- **Python layer:** Scenario runner scripts (best/base/worst) and optional statistical or ML-based forecast components.
- **Documentation:** dbt docs generate + `docs/` content for architecture and scenario definitions.
- **CI:** GitHub Actions (or similar) for `dbt build` and tests on push/PR.
- **Data:** Optional connectors or pipelines for live CRM/usage sources; keep seeds for demos and tests.
- **Marts:** Formal forecast fact tables with scenario keys and versioning.

---

## Repository layout

- `dbt/` — dbt project (models, seeds, macros, tests, profiles).
- `scripts/` — Setup and dbt runner scripts.
- `warehouse/` — Local DuckDB database directory (empty in repo; `*.duckdb` ignored).
- `docs/` — Project and architecture documentation.
