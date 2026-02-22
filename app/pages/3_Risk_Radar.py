"""
Risk Radar page — Churn Risk Watchlist and Top ARR Movers with download CSV.
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
    get_churn_risk_watchlist,
    get_months_for_risk,
    get_top_arr_movers,
)
from src.ui import section_header

if not is_data_available()[0]:
    st.warning("Run dbt + ML pipeline first to populate marts.")
    st.stop()

section_header("Risk Radar", level=1)

# Month list from watchlist
try:
    qm, pm = get_months_for_risk()
    months_df = read_sql(qm, pm)
except Exception:
    months_df = pd.DataFrame()

if months_df.empty or "month" not in months_df.columns:
    st.info("No risk data available. Run dbt to build mart_churn_risk_watchlist.")
    st.stop()

months = months_df["month"].tolist()
month_options = [str(m) for m in months]
month_str = st.selectbox("Month", options=month_options, index=0, key="risk_month")
segment = st.selectbox("Segment", options=["All", "enterprise", "large", "medium", "smb"], index=0, key="risk_segment")

month_val = month_str[:10] if len(month_str) >= 10 else month_str

# Churn risk watchlist (left)
try:
    qw, pw = get_churn_risk_watchlist(month_val, segment)
    df_watch = read_sql(qw, pw)
except Exception:
    df_watch = pd.DataFrame()

# Top ARR movers (right)
try:
    qt, pt = get_top_arr_movers(month_val, segment)
    df_movers = read_sql(qt, pt)
except Exception:
    df_movers = pd.DataFrame()

cols_left, cols_right = st.columns(2)

with cols_left:
    st.markdown("**Churn Risk Watchlist** (top 20)")
    if df_watch.empty:
        st.caption("No watchlist rows for this month/segment.")
    else:
        want_w = ["customer_name", "segment", "months_to_renewal", "current_arr", "p_renew", "health_score_1_10", "slope_bucket", "risk_reason"]
        display_w = df_watch[[c for c in want_w if c in df_watch.columns]].copy()
        display_w = display_w.rename(columns={
            "customer_name": "Customer",
            "segment": "Segment",
            "months_to_renewal": "Months to renewal",
            "current_arr": "Current ARR",
            "p_renew": "P(renew)",
            "health_score_1_10": "Health (1–10)",
            "slope_bucket": "Slope",
            "risk_reason": "Risk reason",
        })
        st.dataframe(display_w, use_container_width=True, hide_index=True)
        csv_w = display_w.to_csv(index=False)
        st.download_button("Download CSV", data=csv_w, file_name="churn_risk_watchlist.csv", mime="text/csv", key="dl_watchlist")

with cols_right:
    st.markdown("**Top ARR Movers** (top 10)")
    if df_movers.empty:
        st.caption("No movers for this month/segment.")
    else:
        want_m = ["customer_name", "arr_delta", "bridge_category", "health_score_1_10", "slope_bucket"]
        display_m = df_movers[[c for c in want_m if c in df_movers.columns]].copy().rename(columns={
            "customer_name": "Customer",
            "arr_delta": "ARR delta",
            "bridge_category": "Bridge category",
            "health_score_1_10": "Health (1–10)",
            "slope_bucket": "Slope",
        })
        st.dataframe(display_m, use_container_width=True, hide_index=True)
        csv_m = display_m.to_csv(index=False)
        st.download_button("Download CSV", data=csv_m, file_name="top_arr_movers.csv", mime="text/csv", key="dl_movers")
