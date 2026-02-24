# Revenue Forecasting Platform — one-command demo and targets.
# Run from repo root. Relative paths only; macOS-friendly.

.PHONY: help setup dbt ml build app demo demo_pack pdf_report clean sim sim_validate sim_validate_warn sim_demo sim_demo_showcase showcase check_dashboard_data

help:
	@echo "Revenue Forecasting Platform — targets (run from repo root):"
	@echo "  make setup     - Create .venv and install requirements.txt (uses scripts/setup.sh)"
	@echo "  make showcase  - RECOMMENDED for audience: sim data + full ML pipeline + open app (good-quality forecast, model stats)"
	@echo "  make sim_demo  - Sim data + validate + dbt + run_all.sh sim (no app launch)"
	@echo "  make app       - Run Streamlit cockpit (streamlit run app/Home.py)"
	@echo "  make dbt       - Run dbt seed, run, test (demo mode; use sim_demo for sim)"
	@echo "  make ml        - Run ML: publish, train, backtests, calibration"
	@echo "  make build     - Full build: dbt (demo) then ml then dbt"
	@echo "  make demo      - build + app (demo seeds only; minimal data, not for audience)"
	@echo "  make demo_pack - Sim data + dbt + ML + export + narrative report"
	@echo "  make pdf_report - Generate PDF report (run after build or sim_demo)"
	@echo "  make sim       - Generate sim data only"
	@echo "  make sim_validate - Validate sim data"
	@echo "  make check_dashboard_data - Verify ARR waterfall and forecast marts have non-zero data (run after showcase/sim_demo)"
	@echo "  make clean     - Remove dbt/target, dbt/logs, warehouse/*.duckdb"

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

demo_pack:
	@./scripts/demo_data_pack.sh --mode sim

pdf_report:
	@./scripts/generate_pdf_report.sh

sim:
	@./scripts/sim_generate.sh

sim_validate:
	@./scripts/sim_validate.sh

sim_validate_warn:
	@./scripts/sim_validate.sh --warn-only

# Absolute sim_data_path so DuckDB views work when app runs from repo root (not just from dbt/).
SIM_DATA_ABS = $(CURDIR)/warehouse/sim_data

sim_demo: sim sim_validate
	@cd dbt && export DBT_PROFILES_DIR=./profiles && ../.venv/bin/dbt seed && ../.venv/bin/dbt run --vars '{"data_mode": "sim", "sim_data_path": "$(SIM_DATA_ABS)"}'
	@./scripts/run_all.sh sim

# Like sim_demo but validation is non-blocking (warn-only) so showcase always proceeds.
sim_demo_showcase: sim sim_validate_warn
	@cd dbt && export DBT_PROFILES_DIR=./profiles && ../.venv/bin/dbt seed && ../.venv/bin/dbt run --vars '{"data_mode": "sim", "sim_data_path": "$(SIM_DATA_ABS)"}'
	@./scripts/run_all.sh sim

# Recommended for audience: good-quality sim data, full ML pipeline, then open app.
showcase: sim_demo_showcase app

# Verify dashboard marts have non-zero data (run after make sim_demo_showcase or make showcase).
check_dashboard_data:
	@.venv/bin/python scripts/check_dashboard_data.py

clean:
	@rm -rf dbt/target dbt/logs
	@rm -f warehouse/*.duckdb
	@echo "Cleaned dbt/target, dbt/logs, warehouse/*.duckdb"
