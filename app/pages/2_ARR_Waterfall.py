"""
ARR Waterfall page — table + waterfall-style bar and reconciliation indicator.
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
    get_available_months,
    get_arr_reconciliation,
    get_arr_waterfall,
)
from src.ui import section_header

if not is_data_available()[0]:
    st.warning("Run dbt + ML pipeline first to populate marts.")
    st.stop()

section_header("ARR Waterfall", level=1)

# Month list for selector
try:
    qm, pm = get_available_months()
    months_df = read_sql(qm, pm)
except Exception:
    months_df = pd.DataFrame()

if months_df.empty or "month" not in months_df.columns:
    st.info("No forecast months available. Run dbt to build marts.")
    run_checklist()
    footer()
    st.stop()

months = months_df["month"].tolist()
month_options = [str(m) for m in months]
default_ix = 0 if month_options else 0

month_str = st.selectbox("Month", options=month_options, index=default_ix, key="arr_month")
scenario = st.selectbox("Scenario", options=["base", "upside", "downside"], index=0, key="arr_scenario")
segment = st.selectbox("Segment", options=["All", "enterprise", "large", "medium", "smb"], index=0, key="arr_segment")

# Parse month for query (DuckDB date)
month_val = month_str
if "T" in month_str or len(month_str) > 10:
    month_val = month_str[:10]

# Reconciliation indicator (optional table)
recon_ok = None
recon_diff = None
try:
    qr, pr = get_arr_reconciliation(month_val, scenario, segment)
    recon_df = read_sql(qr, pr)
    if not recon_df.empty:
        recon_ok = bool(recon_df["ok_flag"].iloc[0]) if "ok_flag" in recon_df.columns else None
        recon_diff = float(recon_df["diff"].iloc[0]) if "diff" in recon_df.columns else None
except Exception:
    pass

if recon_ok is not None:
    if recon_ok:
        st.markdown("**Reconciliation:** ✅ OK")
    else:
        st.markdown(f"**Reconciliation:** ⚠️ Diff = {recon_diff:.2f}" if recon_diff is not None else "**Reconciliation:** ⚠️ Check failed")
st.markdown("---")

# ARR waterfall data
try:
    qw, pw = get_arr_waterfall(month_val, scenario, segment)
    df = read_sql(qw, pw)
except Exception:
    st.warning("Could not load ARR waterfall. Run dbt to build mart_arr_waterfall_monthly.")
    run_checklist()
    footer()
    st.stop()

if df.empty:
    st.info("No ARR waterfall row for this month/scenario/segment.")
    footer()
    st.stop()

row = df.iloc[0]
cols_display = [
    "starting_arr", "new_arr", "expansion_arr", "contraction_arr", "churn_arr",
    "ending_arr", "net_new_arr", "nrr", "grr",
]
table_data = {c: row.get(c) for c in cols_display if c in row}
table_df = pd.DataFrame([table_data])

st.markdown("**Table**")
st.dataframe(
    table_df.rename(columns={
        "starting_arr": "Starting ARR",
        "new_arr": "New",
        "expansion_arr": "Expansion",
        "contraction_arr": "Contraction",
        "churn_arr": "Churn",
        "ending_arr": "Ending ARR",
        "net_new_arr": "Net new",
        "nrr": "NRR",
        "grr": "GRR",
    }),
    use_container_width=True,
    hide_index=True,
)

# Waterfall-like bar: starting, new, expansion, contraction, churn, ending (ordered categories)
try:
    import altair as alt
except ImportError:
    alt = None

bar_order = ["starting", "new", "expansion", "contraction", "churn", "ending"]
bar_values = [
    float(row.get("starting_arr") or 0),
    float(row.get("new_arr") or 0),
    float(row.get("expansion_arr") or 0),
    -float(row.get("contraction_arr") or 0),
    -float(row.get("churn_arr") or 0),
    float(row.get("ending_arr") or 0),
]
bar_df = pd.DataFrame({
    "category": bar_order,
    "value": bar_values,
    "label": ["Starting", "New", "Expansion", "Contraction", "Churn", "Ending"],
})

if alt is not None:
    chart = alt.Chart(bar_df).mark_bar().encode(
        x=alt.X("label:N", sort=bar_df["label"].tolist(), title=""),
        y=alt.Y("value:Q", title="ARR"),
        color=alt.condition(
            alt.datum.value >= 0,
            alt.value("#4a90a4"),
            alt.value("#c25b56"),
        ),
    ).properties(height=320)
    st.altair_chart(chart, use_container_width=True)
else:
    st.bar_chart(bar_df.set_index("label")["value"])
footer(month_val if month_val else None)
