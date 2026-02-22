#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$REPO_ROOT"

export DBT_PROFILES_DIR="${REPO_ROOT}/dbt/profiles"
cd dbt
exec "${REPO_ROOT}/.venv/bin/dbt" run
