"""
Read forecasting/config/model_selection.yml and write preferred model per dataset
to DuckDB table ml_model_selection so dbt can read it without parsing YAML.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd

try:
    import yaml
except ImportError:
    yaml = None

from forecasting.src.io_duckdb import get_warehouse_dir, write_table


def _config_path() -> Path:
    return Path(__file__).resolve().parent.parent / "config" / "model_selection.yml"


def load_config(config_path: Optional[Path] = None) -> dict[str, Any]:
    path = config_path or _config_path()
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    if yaml is None:
        raise RuntimeError("PyYAML is required; pip install pyyaml")
    with open(path) as f:
        return yaml.safe_load(f) or {}


def publish(warehouse_dir: Optional[Path] = None, config_path: Optional[Path] = None) -> None:
    config = load_config(config_path)
    rows = []
    for dataset in ("renewals", "pipeline"):
        pref = (config.get(dataset) or {}).get("preferred_model") or "logistic"
        if pref not in ("logistic", "xgboost"):
            pref = "logistic"
        rows.append({
            "dataset": dataset,
            "preferred_model": pref,
            "updated_at_utc": datetime.now(timezone.utc),
        })
    df = pd.DataFrame(rows)
    write_table(df, "ml_model_selection", mode="replace", warehouse_dir=warehouse_dir)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Publish model selection from YAML to DuckDB for dbt.",
    )
    parser.add_argument(
        "--duckdb-path",
        type=str,
        default="./warehouse/revenue_forecasting.duckdb",
        help="Path to DuckDB database file",
    )
    parser.add_argument(
        "--config-path",
        type=str,
        default=None,
        help="Path to model_selection.yml (default: forecasting/config/model_selection.yml)",
    )
    args = parser.parse_args()
    warehouse_dir = Path(args.duckdb_path).resolve().parent
    config_path = Path(args.config_path).resolve() if args.config_path else None
    publish(warehouse_dir=warehouse_dir, config_path=config_path)


if __name__ == "__main__":
    main()
