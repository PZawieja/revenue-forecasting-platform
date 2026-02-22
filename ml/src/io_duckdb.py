"""DuckDB I/O helpers for reading dbt outputs and writing Parquet."""

from pathlib import Path
from typing import Optional

import pandas as pd

try:
    import duckdb
except ImportError:
    duckdb = None  # type: ignore

from ml.src.utils import get_warehouse_path


def get_duckdb_path() -> Path:
    """Path to the DuckDB database (warehouse/revenue_forecasting.duckdb)."""
    return get_warehouse_path()


def get_connection(duckdb_path: Optional[Path] = None):
    """Return a read-only DuckDB connection to the warehouse."""
    if duckdb is None:
        raise RuntimeError("duckdb package is required; install with pip install duckdb")
    path = duckdb_path or get_duckdb_path()
    return duckdb.connect(str(path), read_only=True)


def read_sql(query: str, duckdb_path: Optional[Path] = None) -> pd.DataFrame:
    """
    Execute a SQL query against the DuckDB warehouse and return a pandas DataFrame.
    Assumes dbt has been run so that models exist as views/tables.
    """
    conn = get_connection(duckdb_path)
    try:
        return conn.execute(query).fetchdf()
    finally:
        conn.close()


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write a DataFrame to Parquet; create parent directories if needed."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
