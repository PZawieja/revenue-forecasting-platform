"""
Light executive UI helpers: metric cards, section headers, run checklist, footer.
Strict light theme; minimal design. All paths relative to repo root.
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


def run_checklist() -> None:
    """Show run checklist when data is missing. Commands are relative to repo root."""
    st.markdown("**Run checklist** (from repo root):")
    st.markdown("1. **dbt seed + run** — build marts:")
    st.code("./scripts/dbt_seed.sh && ./scripts/dbt_run.sh", language="bash")
    st.markdown("2. **ML train + backtest** (optional):")
    st.code("./scripts/run_all.sh", language="bash")
    st.markdown("3. **dbt run + test** — refresh and validate:")
    st.code("./scripts/dbt_run.sh && ./scripts/dbt_test.sh", language="bash")
    st.caption("Then refresh this app.")


def footer(last_updated_month: Optional[str] = None) -> None:
    """Small footer: data source, build, last updated month (if provided)."""
    st.markdown("---")
    parts = ["**Data source:** DuckDB", "**Build:** dbt"]
    if last_updated_month is not None:
        parts.append(f"**Last updated month:** {last_updated_month}")
    st.caption(" · ".join(parts))
