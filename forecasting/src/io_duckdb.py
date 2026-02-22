"""DuckDB I/O: connect to warehouse, read_table(sql), write_table(df, table_name, mode=replace|append)."""

from pathlib import Path
from typing import Literal, Optional

import pandas as pd

try:
    import duckdb
except ImportError:
    duckdb = None  # type: ignore


def get_warehouse_dir(base_path: Optional[Path] = None) -> Path:
    """
    Path to the warehouse directory (contains the DuckDB file).
    Default: ./warehouse relative to base_path or current working directory.
    Do not hardcode absolute paths; use this so scripts work from repo root.
    """
    if base_path is not None:
        p = Path(base_path).resolve()
    else:
        p = Path.cwd()
    return p / "warehouse"


def get_duckdb_path(warehouse_dir: Optional[Path] = None) -> Path:
    """Path to the DuckDB database file (warehouse/revenue_forecasting.duckdb)."""
    wh = warehouse_dir or get_warehouse_dir()
    return wh / "revenue_forecasting.duckdb"


def get_connection(warehouse_dir: Optional[Path] = None, read_only: bool = False):
    """Return a DuckDB connection to the warehouse. read_only=True for read-only."""
    if duckdb is None:
        raise RuntimeError("duckdb is required; install with pip install duckdb")
    path = get_duckdb_path(warehouse_dir)
    return duckdb.connect(str(path), read_only=read_only)


def read_table(sql: str, warehouse_dir: Optional[Path] = None) -> pd.DataFrame:
    """
    Run a SQL query against the DuckDB warehouse and return a pandas DataFrame.
    Example: read_table("SELECT * FROM main.mart_churn_risk_watchlist LIMIT 10")
    """
    conn = get_connection(warehouse_dir, read_only=True)
    try:
        return conn.execute(sql).fetchdf()
    finally:
        conn.close()


def write_table(
    df: pd.DataFrame,
    table_name: str,
    mode: Literal["replace", "append"] = "replace",
    schema: str = "main",
    warehouse_dir: Optional[Path] = None,
) -> None:
    """
    Write a DataFrame to a DuckDB table in the warehouse.
    table_name: unqualified name (e.g. 'renewal_predictions').
    mode: 'replace' creates or overwrites the table; 'append' inserts into existing table.
    """
    if duckdb is None:
        raise RuntimeError("duckdb is required; install with pip install duckdb")
    conn = get_connection(warehouse_dir, read_only=False)
    try:
        conn.register("_write_df", df)
        qualified = f"{schema}.{table_name}" if schema else table_name
        if mode == "replace":
            conn.execute(f"CREATE OR REPLACE TABLE {qualified} AS SELECT * FROM _write_df")
        else:
            conn.execute(f"INSERT INTO {qualified} SELECT * FROM _write_df")
    finally:
        conn.close()
