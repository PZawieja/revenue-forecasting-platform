"""
Light executive UI helpers: metric cards, section headers, run checklist, footer.
Strict light theme; minimal design. All paths relative to repo root.
"""

from typing import Optional, Union

import streamlit as st


def metric_card(label: str, value: Union[str, int, float], delta: Optional[str] = None) -> None:
    """Render a single metric in a consistent card style (light, bordered)."""
    st.metric(label=label, value=value, delta=delta)


def section_header(title: str, level: int = 2) -> None:
    """Render a section header with consistent spacing."""
    st.markdown(f"{'#' * level} {title}")
    st.markdown("---")


def run_checklist() -> None:
    """Show run checklist when data is missing. Commands are relative to repo root."""
    st.markdown("**Run checklist** (from repo root):")
    st.markdown("1. **Recommended** — one command (close this app first so DuckDB is not locked):")
    st.code("make showcase", language="bash")
    st.markdown("2. **Or step-by-step** (sim mode for good-quality data):")
    st.code(
        "make sim\n"
        "cd dbt && DBT_PROFILES_DIR=./profiles ../.venv/bin/dbt run --vars '{data_mode: sim}'\n"
        "./scripts/run_all.sh sim",
        language="bash",
    )
    st.caption("Then refresh this app.")


def footer(last_updated_month: Optional[str] = None) -> None:
    """Small footer: data source, build, last updated month (if provided)."""
    st.markdown("---")
    parts = ["**Data source:** DuckDB", "**Build:** dbt"]
    if last_updated_month is not None:
        parts.append(f"**Last updated month:** {last_updated_month}")
    st.caption(" · ".join(parts))
