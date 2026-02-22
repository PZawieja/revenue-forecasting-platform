#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$REPO_ROOT/dbt"
export DBT_PROFILES_DIR=./profiles
exec "$REPO_ROOT/.venv/bin/dbt" seed
