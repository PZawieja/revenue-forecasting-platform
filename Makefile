# Revenue Forecasting Platform — one-command demo and targets.
# Run from repo root. Relative paths only; macOS-friendly.

.PHONY: help setup dbt ml build app demo clean

help:
	@echo "Revenue Forecasting Platform — targets (run from repo root):"
	@echo "  make setup   - Create .venv and install requirements.txt (uses scripts/setup.sh)"
	@echo "  make dbt    - Run dbt seed, run, test (DBT_PROFILES_DIR=./dbt/profiles)"
	@echo "  make ml     - Run ML: publish selection, train, backtests, calibration (existing scripts)"
	@echo "  make build  - Full build: dbt, then ml, then dbt again"
	@echo "  make app    - Run Streamlit cockpit (streamlit run app/Home.py)"
	@echo "  make demo   - Run build then app (one-command demo)"
	@echo "  make clean  - Remove dbt/target, dbt/logs, and warehouse/*.duckdb (keep warehouse/)"

setup:
	@if [ -f scripts/setup.sh ]; then ./scripts/setup.sh; else python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt; fi

dbt:
	@mkdir -p warehouse
	@cd dbt && export DBT_PROFILES_DIR=./profiles && ../.venv/bin/dbt seed && ../.venv/bin/dbt run && ../.venv/bin/dbt test

ml:
	@./scripts/publish_model_selection.sh
	@./scripts/ml_train_renewals.sh
	@./scripts/ml_train_pipeline.sh
	@./scripts/ml_backtest_renewals.sh
	@./scripts/ml_backtest_pipeline.sh
	@./scripts/ml_calibration_reports.sh

build: dbt ml dbt

app:
	@.venv/bin/streamlit run app/Home.py

demo: build app

clean:
	@rm -rf dbt/target dbt/logs
	@rm -f warehouse/*.duckdb
	@echo "Cleaned dbt/target, dbt/logs, warehouse/*.duckdb"
