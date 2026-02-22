"""
Forecast page — Forecast vs Actual with prediction intervals (segment-aware).
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
    get_forecast_timeseries,
    get_forecast_timeseries_fallback,
    get_latest_confidence,
    get_latest_coverage,
)
from src.ui import section_header

if not is_data_available()[0]:
    st.warning("Run dbt + ML pipeline first to populate marts.")
    st.stop()

section_header("Forecast vs Actual", level=1)

# Controls
scenario = st.selectbox("Scenario", options=["base", "upside", "downside"], index=0, key="forecast_scenario")
segment = st.selectbox(
    "Segment",
    options=["All", "enterprise", "large", "medium", "smb"],
    index=0,
    key="forecast_segment",
)

# Date range: default last 12 months (we filter after load)
months_back = st.number_input("Months to show", min_value=1, max_value=60, value=12, key="forecast_months")

# Load timeseries: try intervals table first, then fallback to monthly
try:
    q, p = get_forecast_timeseries(scenario, segment)
    df = read_sql(q, p)
except Exception:
    df = pd.DataFrame()

if df.empty:
    try:
        q, p = get_forecast_timeseries_fallback(scenario, segment)
        df = read_sql(q, p)
    except Exception:
        df = pd.DataFrame()

if df.empty:
    st.info("No forecast data for this scenario/segment. Run dbt to build forecast marts.")
    st.stop()

# Restrict to last N months
df = df.sort_values("month").reset_index(drop=True)
if len(df) > months_back:
    df = df.tail(months_back).copy()

# Ensure numeric
for col in ["forecast_mrr_total", "actual_mrr", "forecast_lower", "forecast_upper"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# Chart with Altair (minimal styling)
try:
    import altair as alt
except ImportError:
    alt = None

if alt is not None and not df.empty:
    df_chart = df.copy()
    df_chart["month"] = pd.to_datetime(df_chart["month"]).dt.strftime("%Y-%m")
    has_bounds = "forecast_lower" in df_chart.columns and "forecast_upper" in df_chart.columns and df_chart["forecast_lower"].notna().any()
    if has_bounds:
        band = alt.Chart(df_chart).mark_area(opacity=0.2).encode(
            x=alt.X("month:N", title="Month"),
            y=alt.Y("forecast_lower:Q", title="MRR"),
            y2="forecast_upper:Q",
            color=alt.value("#cccccc"),
        )
    line = alt.Chart(df_chart).transform_fold(
        ["actual_mrr", "forecast_mrr_total"],
        as_=["series", "value"],
    ).mark_line(point=True).encode(
        x=alt.X("month:N", title="Month"),
        y=alt.Y("value:Q", title="MRR"),
        color=alt.Color("series:N", legend=alt.Legend(title="")),
    )
    chart = (alt.layer(band, line) if has_bounds else line).properties(height=350)
    st.altair_chart(chart, use_container_width=True)
else:
    st.line_chart(
        df.set_index("month")[["actual_mrr", "forecast_mrr_total"]].rename(columns={"actual_mrr": "Actual", "forecast_mrr_total": "Forecast"}),
        use_container_width=True,
    )

# Compact table: month, actual_mrr, forecast_mrr_total, error, ape, lower, upper
df_table = df.copy()
df_table["error"] = df_table["forecast_mrr_total"] - df_table["actual_mrr"]
df_table["ape"] = (df_table["error"].abs() / df_table["actual_mrr"].replace(0, float("nan"))).fillna(0)
display_cols = ["month", "actual_mrr", "forecast_mrr_total", "error", "ape"]
if "forecast_lower" in df_table.columns and "forecast_upper" in df_table.columns:
    display_cols.extend(["forecast_lower", "forecast_upper"])
st.dataframe(
    df_table[display_cols].rename(columns={
        "actual_mrr": "Actual MRR",
        "forecast_mrr_total": "Forecast MRR",
        "error": "Error",
        "ape": "APE",
        "forecast_lower": "Lower",
        "forecast_upper": "Upper",
    }),
    use_container_width=True,
    hide_index=True,
)

# Interpretation box: 3 bullets from latest month (deterministic)
st.markdown("**Interpretation**")
latest = df.iloc[-1] if len(df) > 0 else None
bullets = []
if latest is not None:
    f = float(latest.get("forecast_mrr_total") or 0)
    a = float(latest.get("actual_mrr") or 0)
    if a != 0:
        pct = (f - a) / a
        if pct > 0.05:
            bullets.append(f"Forecast is {pct:.1%} above actual in the latest month.")
        elif pct < -0.05:
            bullets.append(f"Forecast is {-pct:.1%} below actual in the latest month.")
        else:
            bullets.append("Forecast and actual are within 5% in the latest month.")
    else:
        bullets.append("No actual MRR in the latest month; forecast only.")
    # Confidence and pipeline from latest (we'd need to fetch; use placeholder or skip)
    try:
        qc, pc = get_latest_confidence(scenario)
        conf_df = read_sql(qc, pc)
        if not conf_df.empty and "confidence_score_0_100" in conf_df.columns:
            c = float(conf_df["confidence_score_0_100"].iloc[0] or 0)
            if c < 40:
                bullets.append("Overall confidence score is low (< 40); consider pipeline and renewal assumptions.")
            elif c >= 70:
                bullets.append("Confidence score is solid (≥ 70).")
    except Exception:
        pass
    try:
        qcov, pcov = get_latest_coverage(scenario)
        cov_df = read_sql(qcov, pcov)
        if not cov_df.empty and "pipeline_coverage_ratio" in cov_df.columns:
            r = float(cov_df["pipeline_coverage_ratio"].iloc[0] or 0)
            if r > 0.5:
                bullets.append("Pipeline coverage ratio is high; new business share is material.")
            elif r < 0.1 and len(bullets) < 3:
                bullets.append("Pipeline coverage is low; forecast relies more on renewals and expansion.")
        else:
            if len(bullets) < 3:
                bullets.append("Review coverage metrics in Model Intelligence for pipeline vs renewal mix.")
    except Exception:
        pass
while len(bullets) < 3:
    bullets.append("Run backtests and calibration for more interpretation.")
    break
for b in bullets[:3]:
    st.markdown(f"- {b}")
