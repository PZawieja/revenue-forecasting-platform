"""
Model Intelligence page — champion selection, backtest metrics, calibration summary.
"""

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

_APP_DIR = Path(__file__).resolve().parent.parent
if str(_APP_DIR) not in sys.path:
    sys.path.insert(0, str(_APP_DIR))

from src.db import is_data_available, read_sql
from src.queries import (
    get_latest_backtest_metrics,
    get_latest_calibration_bins,
    get_model_selection,
)
from src.ui import footer, run_checklist, section_header

if not is_data_available()[0]:
    st.warning("Run dbt + ML pipeline first to populate marts.")
    run_checklist()
    footer()
    st.stop()

section_header("Model Intelligence", level=1)
st.markdown("Champion model choice, backtest metrics, and calibration. Use to justify ML choices and monitor forecast quality.")

st.info(
    "**Default pipeline trains logistic regression only** (no XGBoost), so you typically see one model per dataset. "
    "To train and compare XGBoost: from repo root run `./scripts/ml_train_renewals.sh --model both` and "
    "`./scripts/ml_train_pipeline.sh --model both`. On macOS install OpenMP first: `brew install libomp`."
)

# Try ML tables (may not exist)
try:
    q_sel, p_sel = get_model_selection()
    df_sel = read_sql(q_sel, p_sel)
except Exception:
    df_sel = pd.DataFrame()

if df_sel.empty:
    st.info("Run ML training + backtests to populate this page.")
    st.stop()

# Section 1: Champion selection (show all columns: dataset, preferred_model, selection_reason, score_*)
st.markdown("**Champion selection**")
st.caption("Preferred model per dataset (renewals, pipeline). Selection is from backtest performance or config.")
st.dataframe(df_sel, use_container_width=True, hide_index=True)
st.markdown("---")

# Section 2: Backtest metrics (both datasets)
st.markdown("**Backtest metrics** (latest cutoff by segment)")
for dataset in ["renewals", "pipeline"]:
    try:
        q_bt, p_bt = get_latest_backtest_metrics(dataset)
        df_bt = read_sql(q_bt, p_bt)
    except Exception:
        df_bt = pd.DataFrame()
    if df_bt.empty:
        st.caption(f"{dataset}: no backtest metrics.")
        continue
    cols_show = [c for c in ["model_name", "segment", "auc", "brier", "logloss"] if c in df_bt.columns]
    if cols_show:
        st.caption(dataset)
        st.dataframe(df_bt[cols_show], use_container_width=True, hide_index=True)
st.markdown("---")

# Section 3: Calibration chart (p_pred_mean vs y_true_rate + ideal diagonal)
st.markdown("**Calibration** (predicted vs actual rate by bin)")
st.caption("Points should lie near the gray diagonal for a well-calibrated model. Only models that were trained have bins.")

# Preferred model per dataset; only offer models that exist in backtest/calibration data
preferred = {}
if "dataset" in df_sel.columns and "preferred_model" in df_sel.columns:
    for _, r in df_sel.iterrows():
        preferred[str(r["dataset"])] = str(r["preferred_model"])

dataset_for_cal = st.selectbox("Dataset for calibration", options=["renewals", "pipeline"], index=0, key="cal_dataset")
# Build model list from backtest metrics for this dataset so we only show trained models
models_available = ["logistic"]
try:
    q_bt, p_bt = get_latest_backtest_metrics(dataset_for_cal)
    df_bt_mod = read_sql(q_bt, p_bt)
    if not df_bt_mod.empty and "model_name" in df_bt_mod.columns:
        models_available = list(df_bt_mod["model_name"].dropna().unique())
        if "xgboost" in models_available and "logistic" not in models_available:
            models_available = ["xgboost"] + [m for m in models_available if m != "xgboost"]
        elif "logistic" in models_available:
            models_available = ["logistic"] + [m for m in models_available if m != "logistic"]
except Exception:
    pass
default_ix = 0
if preferred.get(dataset_for_cal) in models_available:
    try:
        default_ix = models_available.index(preferred.get(dataset_for_cal))
    except ValueError:
        pass
model_for_cal = st.selectbox(
    "Model",
    options=models_available,
    index=min(default_ix, len(models_available) - 1),
    key="cal_model",
)

try:
    q_cal, p_cal = get_latest_calibration_bins(dataset_for_cal, model_for_cal)
    df_cal = read_sql(q_cal, p_cal)
except Exception:
    df_cal = pd.DataFrame()

with st.expander("Metric glossary"):
    st.markdown("""
- **AUC** — Area under ROC curve; 0.5 = random, 1.0 = perfect discrimination.
- **Brier** — Mean squared error of predicted probabilities; 0 = perfect, 0.25 ≈ random.
- **Log loss** — Logarithmic loss; lower is better; penalizes overconfident wrong predictions.
- **Calibration** — Predicted probability vs actual outcome rate; points on the diagonal = well calibrated.
    """)

if df_cal.empty:
    st.caption("No calibration bins for this dataset/model. Run calibration_reports after backtests.")
else:
    try:
        import altair as alt
    except ImportError:
        alt = None

    df_cal = df_cal.sort_values("bin_id")
    if alt is not None:
        # Points: p_pred_mean vs y_true_rate
        chart_data = df_cal[["bin_id", "p_pred_mean", "y_true_rate"]].copy()
        chart_data["p_pred_mean"] = pd.to_numeric(chart_data["p_pred_mean"], errors="coerce")
        chart_data["y_true_rate"] = pd.to_numeric(chart_data["y_true_rate"], errors="coerce")
        points = alt.Chart(chart_data).mark_point(size=60).encode(
            x=alt.X("p_pred_mean:Q", title="Predicted (mean)", scale=alt.Scale(domain=[0, 1])),
            y=alt.Y("y_true_rate:Q", title="Actual rate", scale=alt.Scale(domain=[0, 1])),
        ).properties(height=300)
        # Ideal diagonal
        diagonal = pd.DataFrame({"x": [0, 1], "y": [0, 1]})
        line = alt.Chart(diagonal).mark_line(color="gray", strokeDash=[4, 2]).encode(
            x=alt.X("x:Q", title="Predicted (mean)"),
            y=alt.Y("y:Q", title="Actual rate"),
        )
        st.altair_chart(alt.layer(line, points), use_container_width=True)
    else:
        st.line_chart(df_cal.set_index("bin_id")[["p_pred_mean", "y_true_rate"]])
    st.caption("Gray dashed line = ideal (perfect calibration).")
footer()
