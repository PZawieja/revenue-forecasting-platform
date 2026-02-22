"""
Revenue Intelligence Executive Cockpit — Home.
Light theme, wide layout; requires DuckDB + dbt marts.
"""

import sys
from pathlib import Path

import streamlit as st

# Ensure app directory is on path when running as streamlit run app/Home.py
_APP_DIR = Path(__file__).resolve().parent
if str(_APP_DIR) not in sys.path:
    sys.path.insert(0, str(_APP_DIR))

from src.db import is_data_available
from src.ui import section_header

st.set_page_config(page_title="Revenue Intelligence Cockpit", layout="wide")

ok, msg = is_data_available()
if not ok:
    st.warning(msg)
    st.stop()

section_header("Revenue Intelligence Executive Cockpit", level=1)
st.markdown("Welcome. Use the sidebar to open **Forecast**, **ARR Waterfall**, **Risk Radar**, or **Model Intelligence**.")
st.markdown("")  # spacing

cols = st.columns(3)
with cols[0]:
    st.metric("Placeholder", "—", None)
with cols[1]:
    st.metric("Placeholder", "—", None)
with cols[2]:
    st.metric("Placeholder", "—", None)
