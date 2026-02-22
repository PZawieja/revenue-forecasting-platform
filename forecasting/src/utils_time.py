"""Time utilities: month boundaries and safe YYYY-MM-01 parsing."""

from datetime import date
from typing import Optional

# Use pandas for month arithmetic when available
try:
    import pandas as pd
    _HAS_PANDAS = True
except ImportError:
    _HAS_PANDAS = False


def parse_month(s: str) -> Optional[date]:
    """
    Parse a string as a month boundary date (YYYY-MM-01).
    Accepts 'YYYY-MM', 'YYYY-MM-01', or 'YYYY-MM-DD' and returns the first day of that month.
    Returns None for invalid or empty input.
    """
    if not s or not isinstance(s, str):
        return None
    s = s.strip()
    if not s:
        return None
    try:
        if len(s) == 7 and s[4] == "-":  # YYYY-MM
            year, month = int(s[:4]), int(s[5:7])
        elif len(s) >= 10 and s[4] == "-" and s[7] == "-":  # YYYY-MM-DD
            year, month = int(s[:4]), int(s[5:7])
        else:
            return None
        if 1 <= month <= 12 and year >= 1900:
            return date(year, month, 1)
    except (ValueError, IndexError):
        pass
    return None


def month_start(d: date) -> date:
    """Return the first day of the month for the given date."""
    return d.replace(day=1)


def add_months(d: date, months: int) -> date:
    """Add months to a date, staying on month boundaries (day=1)."""
    if _HAS_PANDAS:
        t = pd.Timestamp(d) + pd.DateOffset(months=months)
        return t.date()
    # Fallback without pandas: approximate
    year, month = d.year, d.month
    month += months
    while month > 12:
        month -= 12
        year += 1
    while month < 1:
        month += 12
        year -= 1
    return date(year, month, 1)
