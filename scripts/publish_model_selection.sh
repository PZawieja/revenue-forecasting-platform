#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

if [ ! -d "$REPO_ROOT/.venv" ]; then
  echo "No .venv found. Create it first (e.g. ./scripts/setup.sh)." >&2
  exit 1
fi

mkdir -p "$REPO_ROOT/warehouse"

cd "$REPO_ROOT"
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.publish_model_selection --duckdb-path ./warehouse/revenue_forecasting.duckdb

echo "publish_model_selection.sh done."
