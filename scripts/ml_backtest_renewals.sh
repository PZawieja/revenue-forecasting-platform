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

echo "Running renewal ML backtest..."
cd "$REPO_ROOT"
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.backtest_renewals --duckdb-path ./warehouse/revenue_forecasting.duckdb --model logistic

echo "ml_backtest_renewals.sh done."
