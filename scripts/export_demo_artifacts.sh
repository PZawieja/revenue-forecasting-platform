#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

if [ ! -d "$REPO_ROOT/.venv" ]; then
  echo "No .venv found. Run: make setup" >&2
  exit 1
fi

mkdir -p "$REPO_ROOT/docs/artifacts"
cd "$REPO_ROOT"
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.export_artifacts --duckdb-path ./warehouse/revenue_forecasting.duckdb --out-dir ./docs/artifacts
echo "Artifacts written to docs/artifacts/"
