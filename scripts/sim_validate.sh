#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

if [ ! -d "$REPO_ROOT/.venv" ]; then
  echo "No .venv found. Run: make setup" >&2
  exit 1
fi

cd "$REPO_ROOT"
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.sim.src.validate_simulation --config forecasting/sim/config/sim_config.yml
