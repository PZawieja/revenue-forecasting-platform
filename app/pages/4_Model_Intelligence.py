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
st.markdown("Choose champion models and review backtest and calibration. Use to justify ML choices and monitor forecast quality.")

# Try ML tables (may not exist)
try:
    q_sel, p_sel = get_model_selection()
    df_sel = read_sql(q_sel, p_sel)
except Exception:
    df_sel = pd.DataFrame()

if df_sel.empty:
    st.info("Run ML training + backtests to populate this page.")
    st.stop()

# Section 1: Champion selection
st.markdown("**Champion selection**")
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

# Preferred model per dataset for calibration
preferred = {}
if "dataset" in df_sel.columns and "preferred_model" in df_sel.columns:
    for _, r in df_sel.iterrows():
        preferred[str(r["dataset"])] = str(r["preferred_model"])

dataset_for_cal = st.selectbox("Dataset for calibration", options=["renewals", "pipeline"], index=0, key="cal_dataset")
model_for_cal = st.selectbox(
    "Model",
    options=["logistic", "xgboost"],
    index=0 if preferred.get(dataset_for_cal) != "xgboost" else 1,
    key="cal_model",
)

try:
    q_cal, p_cal = get_latest_calibration_bins(dataset_for_cal, model_for_cal)
    df_cal = read_sql(q_cal, p_cal)
except Exception:
    df_cal = pd.DataFrame()

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
