"""
DuckDB connection and query helpers for the Streamlit cockpit.
Uses st.cache_resource for connection, st.cache_data for read_sql.
"""

from pathlib import Path
from typing import Any, Optional

import pandas as pd
import streamlit as st

try:
    import duckdb
except ImportError:
    duckdb = None  # type: ignore


def _repo_root() -> Path:
    """Repo root: directory containing app/ and warehouse/ (or dbt/). Resolved from this file (app/src/db.py)."""
    here = Path(__file__).resolve()
    for p in [here.parent.parent.parent, Path.cwd()]:
        if (p / "warehouse").is_dir() or (p / "dbt").is_dir():
            return p
    return here.parent.parent.parent


def _default_db_path() -> str:
    """Default DuckDB path under repo root so the app finds the DB whether run from repo root or from app/."""
    return str(_repo_root() / "warehouse" / "revenue_forecasting.duckdb")


def get_default_db_path() -> str:
    """Public access to default DuckDB path (e.g. for export pack)."""
    return _default_db_path()


@st.cache_resource
def connect_duckdb(db_path: Optional[str] = None) -> "duckdb.DuckDBPyConnection":
    """Return a DuckDB connection; cached per session. db_path defaults to ./warehouse/revenue_forecasting.duckdb."""
    if duckdb is None:
        raise RuntimeError("duckdb is required; pip install duckdb")
    path = db_path or _default_db_path()
    if not Path(path).exists():
        raise FileNotFoundError(f"DuckDB file not found: {path}")
    return duckdb.connect(path, read_only=True)


@st.cache_data(ttl=60)
def read_sql(query: str, params: Optional[dict[str, Any]] = None, db_path: Optional[str] = None) -> pd.DataFrame:
    """Run a read-only query and return a DataFrame. Cached for 60s."""
    conn = connect_duckdb(db_path)
    if params:
        return conn.execute(query, params).fetchdf()
    return conn.execute(query).fetchdf()


def is_data_available(db_path: Optional[str] = None) -> tuple[bool, str]:
    """
    Return (True, '') if DuckDB exists and at least one mart is readable; else (False, message).
    Use to show a clear message when data is missing.
    """
    path = db_path or _default_db_path()
    if not Path(path).exists():
        return False, "Run dbt + ML pipeline first to populate marts."
    try:
        conn = duckdb.connect(path, read_only=True) if duckdb else None
        if conn is None:
            return False, "Run dbt + ML pipeline first to populate marts."
        conn.execute("SELECT 1 FROM main.mart_executive_forecast_summary LIMIT 1").fetchdf()
        conn.close()
        return True, ""
    except Exception:
        return False, "Run dbt + ML pipeline first to populate marts."
