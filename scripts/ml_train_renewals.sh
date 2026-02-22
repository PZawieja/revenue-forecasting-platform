#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

if [ ! -d "$REPO_ROOT/.venv" ]; then
  echo "No .venv found. Create it first (e.g. ./scripts/setup.sh)." >&2
  exit 1
fi

mkdir -p "$REPO_ROOT/warehouse"

echo "Building ml_features_renewals via dbt..."
cd "$REPO_ROOT/dbt"
export DBT_PROFILES_DIR=./profiles
"$REPO_ROOT/.venv/bin/dbt" seed
"$REPO_ROOT/.venv/bin/dbt" run

echo "Publishing model selection to DuckDB..."
cd "$REPO_ROOT"
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.publish_model_selection --duckdb-path ./warehouse/revenue_forecasting.duckdb

echo "Training renewal ML models and writing ml_renewal_predictions..."
cd "$REPO_ROOT"
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.train_renewals --duckdb-path ./warehouse/revenue_forecasting.duckdb

echo "Rerunning dbt so forecast consumes ML..."
cd "$REPO_ROOT/dbt"
export DBT_PROFILES_DIR=./profiles
"$REPO_ROOT/.venv/bin/dbt" run

echo "ml_train_renewals.sh done."
