"""
Forecast page — placeholder.
"""

import sys
from pathlib import Path

import streamlit as st

_APP_DIR = Path(__file__).resolve().parent.parent
if str(_APP_DIR) not in sys.path:
    sys.path.insert(0, str(_APP_DIR))

from src.db import is_data_available
from src.ui import section_header

if not is_data_available()[0]:
    st.warning("Run dbt + ML pipeline first to populate marts.")
    st.stop()

section_header("Forecast", level=1)
st.markdown("Charts and tables will go here.")
