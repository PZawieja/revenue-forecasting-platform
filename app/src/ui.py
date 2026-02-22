"""
Light executive UI helpers: metric cards, section headers.
Strict light theme; minimal design with subtle borders and spacing.
"""

from typing import Optional

import streamlit as st


def metric_card(label: str, value: str | int | float, delta: Optional[str] = None) -> None:
    """Render a single metric in a consistent card style (light, bordered)."""
    st.metric(label=label, value=value, delta=delta)


def section_header(title: str, level: int = 2) -> None:
    """Render a section header with consistent spacing."""
    st.markdown(f"{'#' * level} {title}")
    st.markdown("---")
