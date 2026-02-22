"""Simulation IO: write Parquet, ensure output dirs. No hardcoded absolute paths."""

from __future__ import annotations

from pathlib import Path
from typing import Union

import pandas as pd


def ensure_dirs(path: Path) -> None:
    """Create parent directories for path if they do not exist."""
    path.parent.mkdir(parents=True, exist_ok=True)


def write_parquet(df: pd.DataFrame, path: Union[str, Path]) -> None:
    """Write DataFrame to Parquet; ensure parent folders exist. Path is relative or absolute."""
    p = Path(path).resolve()
    ensure_dirs(p)
    df.to_parquet(p, index=False)
