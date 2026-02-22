"""
Revenue Intelligence Executive Cockpit — Home (Executive Snapshot).
Light theme, wide layout; reads from DuckDB marts.
"""

import sys
from pathlib import Path

import streamlit as st

_APP_DIR = Path(__file__).resolve().parent
if str(_APP_DIR) not in sys.path:
    sys.path.insert(0, str(_APP_DIR))

from src.db import is_data_available, read_sql
from src.queries import (
    get_available_months,
    get_latest_coverage,
    get_latest_confidence,
    get_latest_exec_summary,
)
from src.ui import run_checklist, section_header, footer

st.set_page_config(page_title="Revenue Intelligence Cockpit", layout="wide")

ok, msg = is_data_available()
if not ok:
    st.warning(msg)
    run_checklist()
    footer()
    st.stop()

section_header("Revenue Intelligence Executive Cockpit", level=1)
st.markdown("One-page snapshot to align on forecast, confidence, and coverage. Use it to decide where to invest and where risk is concentrated.")

# Top controls
scenario = st.selectbox(
    "Scenario",
    options=["base", "upside", "downside"],
    index=0,
    key="home_scenario",
)
segment = st.selectbox(
    "Segment (for detail views)",
    options=["All", "enterprise", "large", "medium", "smb"],
    index=0,
    key="home_segment",
)
st.markdown("---")

# Fetch latest exec summary, confidence, coverage for chosen scenario
q_summary, p_summary = get_latest_exec_summary(scenario)
q_conf, p_conf = get_latest_confidence(scenario)
q_cov, p_cov = get_latest_coverage(scenario)
q_months, p_months = get_available_months()

try:
    df_summary = read_sql(q_summary, p_summary)
    df_conf = read_sql(q_conf, p_conf)
    df_cov = read_sql(q_cov, p_cov)
    df_months = read_sql(q_months, p_months)
except Exception:
    st.warning("Run dbt + ML pipeline first to populate marts.")
    run_checklist()
    footer()
    st.stop()

# Latest month for data freshness
latest_month = None
if not df_months.empty and "month" in df_months.columns:
    latest_month = df_months["month"].iloc[0]

# Four metric cards (latest month for scenario)
forecast_arr = None
mom_growth = None
confidence = None
pipeline_coverage = None

if not df_summary.empty and "total_forecast_revenue" in df_summary.columns:
    mrr = float(df_summary["total_forecast_revenue"].iloc[0] or 0)
    forecast_arr = 12.0 * mrr
if not df_summary.empty and "revenue_growth_mom" in df_summary.columns:
    mom_growth = df_summary["revenue_growth_mom"].iloc[0]
    mom_growth = float(mom_growth) if mom_growth is not None else None
if not df_conf.empty and "confidence_score_0_100" in df_conf.columns:
    confidence = df_conf["confidence_score_0_100"].iloc[0]
    confidence = float(confidence) if confidence is not None else None
if not df_cov.empty and "pipeline_coverage_ratio" in df_cov.columns:
    pipeline_coverage = df_cov["pipeline_coverage_ratio"].iloc[0]
    pipeline_coverage = float(pipeline_coverage) if pipeline_coverage is not None else None

# Render 4 metric cards
cols = st.columns(4)
with cols[0]:
    val = f"{forecast_arr:,.0f}" if forecast_arr is not None else "—"
    st.metric("Forecast ARR (latest month)", val, None)
with cols[1]:
    if mom_growth is not None:
        val = f"{mom_growth:.1%}"
        delta = f"{mom_growth:.1%} MoM" if mom_growth != 0 else None
    else:
        val = "—"
        delta = None
    st.metric("MoM growth", val, delta)
with cols[2]:
    val = f"{confidence:.0f}" if confidence is not None else "—"
    st.metric("Confidence score (0–100)", val, None)
with cols[3]:
    val = f"{pipeline_coverage:.1%}" if pipeline_coverage is not None else "—"
    st.metric("Pipeline coverage ratio", val, None)

# Data freshness and footer
st.markdown("")
if latest_month is not None:
    st.caption(f"**Data freshness:** Latest month in tables: {latest_month}")
else:
    st.caption("**Data freshness:** No months in forecast tables.")
footer(str(latest_month) if latest_month is not None else None)
