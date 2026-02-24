# Runbook: Revenue Forecasting Platform

Single reference for **reproducible runs** and **simple usage**. Use this when you need to set up once, run locally, or hand off to a teammate.

---

## Prerequisites

- **Python 3.9+** (CI uses 3.11)
- **Git**
- No external services; DuckDB is file-based.

---

## First-time setup (reproducible)

From the **repo root**:

```bash
git clone <repo-url> && cd revenue-forecasting-platform
./scripts/setup.sh
```

This creates `.venv` and installs dependencies from `requirements.txt`. For **fully pinned** environments (e.g. production), pin after setup:

```bash
source .venv/bin/activate
pip freeze > requirements-lock.txt   # optional; commit for strict reproducibility
```

---

## How to run: two modes

| Mode | Data source | Use case |
|------|-------------|----------|
| **Demo** | CSV seeds in `dbt/seeds/` | Quick local demo, **minimal data** (few companies, few months). Numbers can look flat or zero; good for smoke-testing only. |
| **Sim** | Parquet in `warehouse/sim_data/` | **Realistic volume and variance**; use this for believable forecasts, ARR movement, and ML metrics. |

---

## One-command flows

From repo root (after `./scripts/setup.sh`):

| Goal | Command |
|------|---------|
| **Showcase for audience** (sim + full ML + open app) | `make showcase` |
| **Sim pipeline only** (no app) | `make sim_demo` or `./scripts/run_all.sh sim` |
| **Demo data pack** (sim + dbt + ML + export + report) | `make demo_pack` |
| **Quick smoke-test** (demo seeds + app) | `make demo` |
| **Sim: generate + validate only** | `make sim` then `make sim_validate` |
| **Rules-only forecast** (no ML) | `make dbt` |
| **Open Streamlit cockpit** | `make app` or `streamlit run app/Home.py` |
| **Generate PDF report** | After `make sim_demo` or `make build`: `make pdf_report` |

---

## Manual dbt (when not using Make/scripts)

From repo root, use an **absolute** profile path so it works from any directory:

```bash
export DBT_PROFILES_DIR="$(pwd)/dbt/profiles"
cd dbt
../.venv/bin/dbt debug
../.venv/bin/dbt seed          # demo only; skip in sim
../.venv/bin/dbt run           # demo
# Or for sim (use absolute sim_data_path so the app can read the DB from repo root):
../.venv/bin/dbt run --vars "{\"data_mode\": \"sim\", \"sim_data_path\": \"$(cd .. && pwd)/warehouse/sim_data\"}"
../.venv/bin/dbt test
```

When running **from inside `dbt/`** (e.g. in scripts), use:

```bash
export DBT_PROFILES_DIR=./profiles
```

---

## Where outputs go

| Output | Location |
|--------|----------|
| DuckDB warehouse | `warehouse/revenue_forecasting.duckdb` |
| Sim Parquet inputs | `warehouse/sim_data/*.parquet` |
| dbt artifacts | `dbt/target/`, `dbt/logs/` |
| Exported CSVs | `docs/artifacts/*.csv` |
| Narrative report | `docs/reports/revenue_intelligence_report.md` |
| PDF report | `docs/reports/revenue_intelligence_report.pdf` |
| ML model selection (in DB) | table `ml_model_selection` |

---

## Lineage and docs

Generate dbt lineage (no server):

```bash
cd dbt && export DBT_PROFILES_DIR=./profiles && ../.venv/bin/dbt docs generate
```

Serve lineage UI locally:

```bash
cd dbt && ../.venv/bin/dbt docs serve
```

---

## Troubleshooting

| Issue | Action |
|-------|--------|
| "No .venv found" | Run `./scripts/setup.sh` from repo root. |
| dbt "profile not found" | From repo root: `export DBT_PROFILES_DIR="$(pwd)/dbt/profiles"` then `cd dbt` and run dbt. |
| Sim: "missing Parquet" | Run `./scripts/sim_generate.sh` before `run_all.sh sim` or `dbt run --vars '{"data_mode": "sim"}'`. |
| App: "Run checklist" / no data | Run **`make showcase`** from repo root (recommended), or `make build` / `./scripts/run_all.sh sim`. Close the app first if it's open, then run the pipeline; the app resolves the DB from repo root. |
| Switching demo ↔ sim | Sim does not use seeds; for demo after sim, run `dbt seed --full-refresh` then `dbt run` (no vars). |
| XGBoost "libomp.dylib could not be loaded" (macOS) | Scripts and `make ml` use **logistic** only by default, so no XGBoost. To train/backtest XGBoost: `brew install libomp`, then run with `--model both` (e.g. `python -m forecasting.src.train_renewals --model both`). |

---

## CI

- **dbt job:** `dbt seed --full-refresh`, `dbt run`, `dbt test` (demo mode).
- **ml_ci job (optional):** After dbt, runs publish_model_selection → train (both) → backtests → calibration → quality gates → dbt run + test.

See `.github/workflows/dbt_ci.yml` and README "CI" / "CI ML checks" sections.

---

## See also

- [README](../README.md) — Quickstart, roadmap, run types, ML model quality
- [Architecture overview](architecture_overview.md) — Design and diagram
- [Configuration](configuration.md) — Config-driven behavior
