"""
Revenue Intelligence Executive Cockpit — Home (Executive Snapshot).
Light theme, wide layout; reads from DuckDB marts.
"""

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

_APP_DIR = Path(__file__).resolve().parent
if str(_APP_DIR) not in sys.path:
    sys.path.insert(0, str(_APP_DIR))

from src.db import get_default_db_path, is_data_available, read_sql
from src.queries import (
    get_available_months,
    get_forecast_timeseries,
    get_forecast_timeseries_fallback,
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
with st.expander("What's in this cockpit"):
    st.markdown("""
- **Home** — Executive metrics and forecast trend (this page).
- **Forecast vs Actual** — Time series with intervals and variance.
- **ARR Waterfall** — Month-over-month ARR bridge (new, expansion, churn) and last 6 months summary.
- **Risk Radar** — Churn risk watchlist and top ARR movers by segment.
- **Model Intelligence** — Champion model (logistic vs XGBoost), backtest metrics, and calibration.
    """)
st.caption(
    "**Data:** For impressive forecasts and model stats (champion selection, backtest metrics, calibration), use **sim** mode: "
    "from repo root run `make showcase` (close this app first so DuckDB is not locked). Demo (seed) data is minimal and can show zeros."
)

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

# Five metric cards (latest month for scenario)
forecast_arr = None
actual_arr = None
mom_growth = None
confidence = None
pipeline_coverage = None

if not df_summary.empty and "total_forecast_revenue" in df_summary.columns:
    mrr = float(df_summary["total_forecast_revenue"].iloc[0] or 0)
    forecast_arr = 12.0 * mrr
if not df_summary.empty and "total_actual_revenue" in df_summary.columns:
    actual_mrr = float(df_summary["total_actual_revenue"].iloc[0] or 0)
    actual_arr = 12.0 * actual_mrr
if not df_summary.empty and "revenue_growth_mom" in df_summary.columns:
    mom_growth = df_summary["revenue_growth_mom"].iloc[0]
    mom_growth = float(mom_growth) if mom_growth is not None else None
if not df_conf.empty and "confidence_score_0_100" in df_conf.columns:
    confidence = df_conf["confidence_score_0_100"].iloc[0]
    confidence = float(confidence) if confidence is not None else None
if not df_cov.empty and "pipeline_coverage_ratio" in df_cov.columns:
    pipeline_coverage = df_cov["pipeline_coverage_ratio"].iloc[0]
    pipeline_coverage = float(pipeline_coverage) if pipeline_coverage is not None else None

# Render 5 metric cards
cols = st.columns(5)
with cols[0]:
    val = f"{forecast_arr:,.0f}" if forecast_arr is not None else "—"
    st.metric("Forecast ARR (latest month)", val, None)
with cols[1]:
    val = f"{actual_arr:,.0f}" if actual_arr is not None else "—"
    st.metric("Actual ARR (latest month)", val, None)
with cols[2]:
    if mom_growth is not None:
        val = f"{mom_growth:.1%}"
        delta = f"{mom_growth:.1%} MoM" if mom_growth != 0 else None
    else:
        val = "—"
        delta = None
    st.metric("MoM growth", val, delta)
with cols[3]:
    val = f"{confidence:.0f}" if confidence is not None else "—"
    st.metric("Confidence (0–100)", val, None)
with cols[4]:
    val = f"{pipeline_coverage:.1%}" if pipeline_coverage is not None else "—"
    st.metric("Pipeline coverage", val, None)

# Forecast trend (last 12 months): compact line chart
try:
    q_ts, p_ts = get_forecast_timeseries(scenario, segment)
    df_ts = read_sql(q_ts, p_ts)
except Exception:
    df_ts = None
if df_ts is None or df_ts.empty:
    try:
        q_ts, p_ts = get_forecast_timeseries_fallback(scenario, segment)
        df_ts = read_sql(q_ts, p_ts)
    except Exception:
        df_ts = None
if df_ts is not None and not df_ts.empty:
    df_ts = df_ts.sort_values("month").reset_index(drop=True).tail(12)
    for col in ["forecast_mrr_total", "actual_mrr"]:
        if col in df_ts.columns:
            df_ts[col] = pd.to_numeric(df_ts[col], errors="coerce")
    try:
        import altair as alt
        line_forecast = alt.Chart(df_ts).mark_line(point=True, color="#1f77b4").encode(
            x=alt.X("month:T", title="Month"),
            y=alt.Y("forecast_mrr_total:Q", title="MRR"),
        )
        line_actual = alt.Chart(df_ts).mark_line(point=True, color="#ff7f0e", strokeDash=[4, 2]).encode(
            x=alt.X("month:T", title="Month"),
            y=alt.Y("actual_mrr:Q", title="MRR"),
        )
        chart = alt.layer(line_forecast, line_actual).resolve_scale(y="shared").properties(
            title="Forecast vs actual (last 12 months)",
            height=220,
        )
        # Legend: blue = Forecast, orange dashed = Actual
        st.altair_chart(chart, use_container_width=True)
        st.caption("Blue = Forecast MRR · Orange dashed = Actual MRR")
    except Exception:
        pass

# Data freshness
st.markdown("")
if latest_month is not None:
    st.caption(f"**Data freshness:** Latest month in tables: {latest_month}")
else:
    st.caption("**Data freshness:** No months in forecast tables.")

# --- Export Pack (near bottom) ---
st.markdown("---")
section_header("Export Pack", level=2)
st.markdown("Generate demo artifact CSVs, narrative Markdown report, and PDF report in one click. Files are written under `docs/artifacts/` and `docs/reports/`.")

exp_col1, exp_col2, exp_col3 = st.columns([1, 1, 2])
with exp_col1:
    export_scenario = st.selectbox(
        "Scenario (for report)",
        options=["base", "upside", "downside"],
        index=["base", "upside", "downside"].index(scenario) if scenario in ["base", "upside", "downside"] else 0,
        key="export_scenario",
    )
with exp_col2:
    export_months = st.number_input("Months (for report)", min_value=1, max_value=24, value=6, key="export_months")

generate_clicked = st.button("Generate Export Pack", type="primary", key="generate_export_pack")

if generate_clicked:
    from src.export_pack import generate_export_pack

    db_path = get_default_db_path()
    with st.spinner("Generating CSVs, Markdown, and PDF…"):
        try:
            result = generate_export_pack(db_path=db_path, scenario=export_scenario, months=export_months)
        except Exception as e:
            st.error(f"Export pack failed: {e}")
            result = {"artifacts": [], "reports": [], "errors": [str(e)], "zip_path": None}
    st.session_state["export_pack_result"] = result

# Show last result and download buttons (persisted so downloads remain available after rerun)
if st.session_state.get("export_pack_result"):
    result = st.session_state["export_pack_result"]
    if result["errors"]:
        for err in result["errors"]:
            st.warning(f"⚠ {err}")
    if result["artifacts"] or result["reports"]:
        st.success("Export pack generated.")
        if result["artifacts"]:
            st.caption(f"**CSVs:** {len(result['artifacts'])} file(s) in `docs/artifacts/`")
        if result["reports"]:
            st.caption(f"**Reports:** " + ", ".join(result["reports"]))

    if result.get("reports"):
        st.markdown("**Download**")
        dl1, dl2 = st.columns(2)
        md_path = next((p for p in result["reports"] if p.endswith(".md")), None)
        pdf_path = next((p for p in result["reports"] if p.endswith(".pdf")), None)
        with dl1:
            if md_path and Path(md_path).exists():
                with open(md_path, "r", encoding="utf-8") as f:
                    st.download_button("Download Markdown report", data=f.read(), file_name="revenue_intelligence_report.md", mime="text/markdown", key="dl_md")
        with dl2:
            if pdf_path and Path(pdf_path).exists():
                with open(pdf_path, "rb") as f:
                    st.download_button("Download PDF report", data=f.read(), file_name="revenue_intelligence_report.pdf", mime="application/pdf", key="dl_pdf")
    if result.get("zip_path") and Path(result["zip_path"]).exists():
        with open(result["zip_path"], "rb") as f:
            st.download_button("Download CSVs (ZIP)", data=f.read(), file_name="export_pack.zip", mime="application/zip", key="dl_zip")

footer(str(latest_month) if latest_month is not None else None)
