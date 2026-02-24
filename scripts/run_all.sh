#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

# Optional first argument: demo (default) | sim
MODE="${1:-demo}"
if [ "$MODE" != "demo" ] && [ "$MODE" != "sim" ]; then
  echo "Usage: $0 [demo|sim]" >&2
  echo "  demo (default): dbt seed + dbt run with default vars" >&2
  echo "  sim: ./scripts/sim_generate.sh then dbt run with data_mode=sim" >&2
  exit 1
fi

if [ ! -d "$REPO_ROOT/.venv" ]; then
  echo "No .venv found. Create it first (e.g. ./scripts/setup.sh)." >&2
  exit 1
fi

mkdir -p "$REPO_ROOT/warehouse"

if [ "$MODE" = "sim" ]; then
  echo "Generating sim data..."
  "$SCRIPT_DIR/sim_generate.sh"
  echo "Building feature tables via dbt (sim mode, no seed)..."
else
  echo "Building feature tables via dbt (seed + run)..."
fi

cd "$REPO_ROOT/dbt"
export DBT_PROFILES_DIR=./profiles
# Use absolute sim_data_path so DuckDB views work when app runs from repo root.
SIM_DATA_ABS="$REPO_ROOT/warehouse/sim_data"
# Seed config tables in both modes so segment_config, scenario_config, etc. exist (sim uses company_id 1).
"$REPO_ROOT/.venv/bin/dbt" seed
if [ "$MODE" = "sim" ]; then
  "$REPO_ROOT/.venv/bin/dbt" run --vars "{\"data_mode\": \"sim\", \"sim_data_path\": \"$SIM_DATA_ABS\"}"
else
  "$REPO_ROOT/.venv/bin/dbt" run
fi

echo "Publishing model selection to DuckDB..."
cd "$REPO_ROOT"
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.publish_model_selection --duckdb-path ./warehouse/revenue_forecasting.duckdb

echo "Training renewal ML models..."
cd "$REPO_ROOT"
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.train_renewals --duckdb-path ./warehouse/revenue_forecasting.duckdb --model logistic

echo "Training pipeline close-probability ML..."
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.train_pipeline --duckdb-path ./warehouse/revenue_forecasting.duckdb --model logistic

echo "Rerunning dbt so forecast consumes ML..."
cd "$REPO_ROOT/dbt"
export DBT_PROFILES_DIR=./profiles
if [ "$MODE" = "sim" ]; then
  "$REPO_ROOT/.venv/bin/dbt" run --vars "{\"data_mode\": \"sim\", \"sim_data_path\": \"$SIM_DATA_ABS\"}"
else
  "$REPO_ROOT/.venv/bin/dbt" run
fi

echo "Running backtests (renewals + pipeline)..."
cd "$REPO_ROOT"
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.backtest_renewals --duckdb-path ./warehouse/revenue_forecasting.duckdb --model logistic
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.backtest_pipeline --duckdb-path ./warehouse/revenue_forecasting.duckdb --model logistic

echo "Building calibration and cost-curve reports..."
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.calibration_reports --duckdb-path ./warehouse/revenue_forecasting.duckdb

echo "Selecting champion model per dataset from backtest performance..."
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.select_champion_model --duckdb-path ./warehouse/revenue_forecasting.duckdb

echo "Rerunning dbt so forecast uses champion model..."
cd "$REPO_ROOT/dbt"
export DBT_PROFILES_DIR=./profiles
if [ "$MODE" = "sim" ]; then
  "$REPO_ROOT/.venv/bin/dbt" run --vars "{\"data_mode\": \"sim\", \"sim_data_path\": \"$SIM_DATA_ABS\"}"
else
  "$REPO_ROOT/.venv/bin/dbt" run
fi

echo "run_all.sh done ($MODE mode)."
