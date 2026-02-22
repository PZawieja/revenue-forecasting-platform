"""Path and config helpers for the ML module."""

from pathlib import Path

# Repo root: assume we run from repo root (python -m ml.src.xxx) or from ml/
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def get_repo_root() -> Path:
    """Return the repository root directory."""
    return _REPO_ROOT


def get_warehouse_path() -> Path:
    """Return path to the DuckDB warehouse file."""
    return _REPO_ROOT / "warehouse" / "revenue_forecasting.duckdb"


def get_duckdb_table_name(model_name: str) -> str:
    """Return the DuckDB table/view name for a dbt model (default schema)."""
    return model_name
