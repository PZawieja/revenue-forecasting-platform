#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

# Defaults
MODE="sim"
SCENARIO="base"
MONTHS="6"

# Parse optional args
while [ $# -gt 0 ]; do
  case "$1" in
    --mode)
      MODE="${2:?Missing value for --mode}"
      shift 2
      ;;
    --scenario)
      SCENARIO="${2:?Missing value for --scenario}"
      shift 2
      ;;
    --months)
      MONTHS="${2:?Missing value for --months}"
      shift 2
      ;;
    *)
      echo "Usage: $0 [--mode sim|demo] [--scenario base] [--months 6]" >&2
      echo "  --mode    sim (default) or demo" >&2
      echo "  --scenario  scenario name (default base)" >&2
      echo "  --months  months for narrative report (default 6)" >&2
      exit 1
      ;;
  esac
done

if [ "$MODE" != "sim" ] && [ "$MODE" != "demo" ]; then
  echo "Invalid --mode. Use sim or demo." >&2
  exit 1
fi

if [ ! -d "$REPO_ROOT/.venv" ]; then
  echo "No .venv found. Run: make setup" >&2
  exit 1
fi

SECONDS=0
cd "$REPO_ROOT"
mkdir -p warehouse docs/artifacts docs/reports

echo "=============================================="
echo "Demo Data Pack — mode=$MODE scenario=$SCENARIO months=$MONTHS"
echo "=============================================="

if [ "$MODE" = "sim" ]; then
  TOTAL=11
  echo "[1/$TOTAL] Generating sim data..."
  "$SCRIPT_DIR/sim_generate.sh"
  echo "  Step 1 done (${SECONDS}s elapsed)."

  echo "[2/$TOTAL] Validating sim realism..."
  "$SCRIPT_DIR/sim_validate.sh"
  echo "  Step 2 done (${SECONDS}s elapsed)."

  echo "[3/$TOTAL] Building dbt marts (sim mode)..."
  cd "$REPO_ROOT/dbt"
  export DBT_PROFILES_DIR=./profiles
  "$REPO_ROOT/.venv/bin/dbt" run --vars '{data_mode: sim}'
  "$REPO_ROOT/.venv/bin/dbt" test
  cd "$REPO_ROOT"
  echo "  Step 3 done (${SECONDS}s elapsed)."
  STEP=4
else
  TOTAL=9
  echo "[1/$TOTAL] Seeding and building dbt marts (demo mode)..."
  cd "$REPO_ROOT/dbt"
  export DBT_PROFILES_DIR=./profiles
  "$REPO_ROOT/.venv/bin/dbt" seed --full-refresh
  "$REPO_ROOT/.venv/bin/dbt" run
  "$REPO_ROOT/.venv/bin/dbt" test
  cd "$REPO_ROOT"
  echo "  Step 1 done (${SECONDS}s elapsed)."
  STEP=2
fi

echo "[$STEP/$TOTAL] Publishing model selection defaults..."
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.publish_model_selection --duckdb-path ./warehouse/revenue_forecasting.duckdb
echo "  Step $STEP done (${SECONDS}s elapsed)."
STEP=$((STEP + 1))

echo "[$STEP/$TOTAL] Training renewal and pipeline models (both)..."
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.train_renewals --duckdb-path ./warehouse/revenue_forecasting.duckdb --model both
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.train_pipeline --duckdb-path ./warehouse/revenue_forecasting.duckdb --model both
echo "  Step $STEP done (${SECONDS}s elapsed)."
STEP=$((STEP + 1))

echo "[$STEP/$TOTAL] Running backtests (renewals + pipeline)..."
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.backtest_renewals --duckdb-path ./warehouse/revenue_forecasting.duckdb --model both
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.backtest_pipeline --duckdb-path ./warehouse/revenue_forecasting.duckdb --model both
echo "  Step $STEP done (${SECONDS}s elapsed)."
STEP=$((STEP + 1))

echo "[$STEP/$TOTAL] Building calibration reports..."
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.calibration_reports --duckdb-path ./warehouse/revenue_forecasting.duckdb
echo "  Step $STEP done (${SECONDS}s elapsed)."
STEP=$((STEP + 1))

echo "[$STEP/$TOTAL] Selecting champion models..."
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.select_champion_model --duckdb-path ./warehouse/revenue_forecasting.duckdb
echo "  Step $STEP done (${SECONDS}s elapsed)."
STEP=$((STEP + 1))

echo "[$STEP/$TOTAL] Rerunning dbt so forecasts use champion models..."
cd "$REPO_ROOT/dbt"
export DBT_PROFILES_DIR=./profiles
if [ "$MODE" = "sim" ]; then
  "$REPO_ROOT/.venv/bin/dbt" run --vars '{data_mode: sim}'
  "$REPO_ROOT/.venv/bin/dbt" test
else
  "$REPO_ROOT/.venv/bin/dbt" run
  "$REPO_ROOT/.venv/bin/dbt" test
fi
cd "$REPO_ROOT"
echo "  Step $STEP done (${SECONDS}s elapsed)."
STEP=$((STEP + 1))

echo "[$STEP/$TOTAL] Exporting demo artifacts (CSV)..."
"$SCRIPT_DIR/export_demo_artifacts.sh"
echo "  Step $STEP done (${SECONDS}s elapsed)."
STEP=$((STEP + 1))

echo "[$STEP/$TOTAL] Generating narrative report..."
PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv/bin/python" -m forecasting.src.narrative_report \
  --duckdb-path ./warehouse/revenue_forecasting.duckdb \
  --scenario "$SCENARIO" \
  --segment All \
  --months "$MONTHS" \
  --output ./docs/reports/revenue_intelligence_report.md
echo "  Step $STEP done (${SECONDS}s elapsed)."

echo "=============================================="
echo "Demo data pack complete. Total time: ${SECONDS}s"
echo "  Outputs: docs/artifacts/*.csv, docs/reports/revenue_intelligence_report.md"
echo "  Run Streamlit cockpit: make app  (or streamlit run app/Home.py)"
echo "=============================================="
