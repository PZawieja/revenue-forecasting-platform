#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

if [ ! -d "$REPO_ROOT/.venv" ]; then
  echo "No .venv found. Create it first (e.g. ./scripts/setup.sh)." >&2
  exit 1
fi

mkdir -p "$REPO_ROOT/warehouse"

echo "Building feature tables via dbt (seed + run)..."
cd "$REPO_ROOT/dbt"
export DBT_PROFILES_DIR=./profiles
"$REPO_ROOT/.venv/bin/dbt" seed
"$REPO_ROOT/.venv/bin/dbt" run

echo "Publishing model selection to DuckDB..."
cd "$REPO_ROOT"
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.publish_model_selection --duckdb-path ./warehouse/revenue_forecasting.duckdb

echo "Training renewal ML models..."
cd "$REPO_ROOT"
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.train_renewals --duckdb-path ./warehouse/revenue_forecasting.duckdb

echo "Training pipeline close-probability ML..."
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.train_pipeline --duckdb-path ./warehouse/revenue_forecasting.duckdb

echo "Rerunning dbt so forecast consumes ML..."
cd "$REPO_ROOT/dbt"
export DBT_PROFILES_DIR=./profiles
"$REPO_ROOT/.venv/bin/dbt" run

echo "Running backtests (renewals + pipeline)..."
cd "$REPO_ROOT"
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.backtest_renewals --duckdb-path ./warehouse/revenue_forecasting.duckdb
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.backtest_pipeline --duckdb-path ./warehouse/revenue_forecasting.duckdb

echo "Building calibration and cost-curve reports..."
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.calibration_reports --duckdb-path ./warehouse/revenue_forecasting.duckdb

echo "Selecting champion model per dataset from backtest performance..."
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.select_champion_model --duckdb-path ./warehouse/revenue_forecasting.duckdb

echo "Rerunning dbt so forecast uses champion model..."
cd "$REPO_ROOT/dbt"
export DBT_PROFILES_DIR=./profiles
"$REPO_ROOT/.venv/bin/dbt" run

echo "run_all.sh done."
