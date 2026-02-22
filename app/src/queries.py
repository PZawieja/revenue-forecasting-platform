"""
SQL query helpers for cockpit datasets. No hardcoded absolute paths.
Queries reference main schema marts built by dbt.
Uses DuckDB $param for dict parameters.
"""

from typing import Any, Optional


def get_latest_exec_summary(scenario: str) -> tuple[str, dict[str, Any]]:
    """Latest month row from mart_executive_forecast_summary for chosen scenario (aggregated across companies)."""
    sql = """
    SELECT
        max(month) AS month,
        sum(total_forecast_revenue) AS total_forecast_revenue,
        sum(total_actual_revenue) AS total_actual_revenue,
        avg(revenue_growth_mom) AS revenue_growth_mom,
        avg(avg_confidence_score) AS avg_confidence_score
    FROM main.mart_executive_forecast_summary
    WHERE scenario = $scenario AND month = (
        SELECT max(month) FROM main.mart_executive_forecast_summary WHERE scenario = $scenario
    )
    GROUP BY scenario
    """
    return sql.strip(), {"scenario": scenario}


def get_latest_confidence(scenario: str) -> tuple[str, dict[str, Any]]:
    """Latest month confidence score aggregated across segments (int_forecast_confidence)."""
    sql = """
    SELECT
        max(month) AS month,
        avg(confidence_score_0_100) AS confidence_score_0_100
    FROM main.int_forecast_confidence
    WHERE scenario = $scenario AND month = (
        SELECT max(month) FROM main.int_forecast_confidence WHERE scenario = $scenario
    )
    GROUP BY scenario
    """
    return sql.strip(), {"scenario": scenario}


def get_latest_coverage(scenario: str) -> tuple[str, dict[str, Any]]:
    """Latest month coverage ratios from mart_forecast_coverage_metrics (avg across segment/company)."""
    sql = """
    SELECT
        max(month) AS month,
        avg(pipeline_coverage_ratio) AS pipeline_coverage_ratio,
        avg(renewal_coverage_ratio) AS renewal_coverage_ratio
    FROM main.mart_forecast_coverage_metrics
    WHERE scenario = $scenario AND month = (
        SELECT max(month) FROM main.mart_forecast_coverage_metrics WHERE scenario = $scenario
    )
    GROUP BY scenario
    """
    return sql.strip(), {"scenario": scenario}


def get_available_months() -> tuple[str, dict[str, Any]]:
    """Distinct months available in forecast data (from executive summary mart)."""
    sql = """
    SELECT DISTINCT month
    FROM main.mart_executive_forecast_summary
    ORDER BY month DESC
    """
    return sql.strip(), {}


# Legacy placeholders (for other pages)
def sql_executive_forecast_summary() -> str:
    return "SELECT * FROM main.mart_executive_forecast_summary LIMIT 100"


def sql_arr_waterfall() -> str:
    return "SELECT * FROM main.mart_arr_waterfall_monthly LIMIT 100"


def sql_churn_risk_watchlist() -> str:
    return "SELECT * FROM main.mart_churn_risk_watchlist LIMIT 100"


def sql_ml_calibration_bins() -> str:
    return "SELECT * FROM main.mart_ml_calibration_bins LIMIT 100"
