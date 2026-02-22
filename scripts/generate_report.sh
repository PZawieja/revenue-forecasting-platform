#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

if [ ! -d "$REPO_ROOT/.venv" ]; then
  echo "No .venv found. Create it first (e.g. ./scripts/setup.sh)." >&2
  exit 1
fi

cd "$REPO_ROOT"
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.narrative_report \
  --duckdb-path ./warehouse/revenue_forecasting.duckdb \
  --scenario base \
  --segment All \
  --months 6 \
  --output ./docs/reports/revenue_intelligence_report.md
