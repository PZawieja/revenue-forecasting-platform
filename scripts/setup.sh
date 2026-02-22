#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$REPO_ROOT"

python3 -m venv .venv
# shellcheck source=/dev/null
source .venv/bin/activate
pip install -r requirements.txt

echo "Setup complete. Activate the venv with: source .venv/bin/activate"
