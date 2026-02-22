"""
SQL query placeholders for cockpit datasets. No hardcoded absolute paths.
Queries reference main schema marts built by dbt.
"""


def sql_executive_forecast_summary() -> str:
    """Placeholder: executive forecast summary mart."""
    return "SELECT * FROM main.mart_executive_forecast_summary LIMIT 100"


def sql_arr_waterfall() -> str:
    """Placeholder: ARR waterfall monthly."""
    return "SELECT * FROM main.mart_arr_waterfall_monthly LIMIT 100"


def sql_churn_risk_watchlist() -> str:
    """Placeholder: churn risk watchlist."""
    return "SELECT * FROM main.mart_churn_risk_watchlist LIMIT 100"


def sql_ml_calibration_bins() -> str:
    """Placeholder: ML calibration bins."""
    return "SELECT * FROM main.mart_ml_calibration_bins LIMIT 100"
