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


def get_forecast_timeseries(scenario: str, segment: str) -> tuple[str, dict[str, Any]]:
    """
    Timeseries from fct_revenue_forecast_with_intervals: month, segment, scenario,
    forecast_mrr_total, actual_mrr, forecast_lower, forecast_upper.
    segment='All' aggregates across segments. Use this first; fallback to get_forecast_timeseries_fallback if table missing.
    """
    if segment and segment != "All":
        sql = """
        SELECT
            month,
            segment,
            scenario,
            sum(forecast_mrr_total) AS forecast_mrr_total,
            sum(actual_mrr) AS actual_mrr,
            sum(forecast_lower) AS forecast_lower,
            sum(forecast_upper) AS forecast_upper
        FROM main.fct_revenue_forecast_with_intervals
        WHERE scenario = $scenario AND segment = $segment
        GROUP BY month, segment, scenario
        ORDER BY month
        """
        return sql.strip(), {"scenario": scenario, "segment": segment}
    sql = """
    SELECT
        month,
        'All' AS segment,
        scenario,
        sum(forecast_mrr_total) AS forecast_mrr_total,
        sum(actual_mrr) AS actual_mrr,
        sum(forecast_lower) AS forecast_lower,
        sum(forecast_upper) AS forecast_upper
    FROM main.fct_revenue_forecast_with_intervals
    WHERE scenario = $scenario
    GROUP BY month, scenario
    ORDER BY month
    """
    return sql.strip(), {"scenario": scenario}


def get_forecast_timeseries_fallback(scenario: str, segment: str) -> tuple[str, dict[str, Any]]:
    """
    Timeseries from fct_revenue_forecast_monthly (no intervals). forecast_lower/forecast_upper not available (null).
    """
    if segment and segment != "All":
        sql = """
        SELECT
            month,
            segment,
            scenario,
            sum(forecast_mrr_total) AS forecast_mrr_total,
            sum(actual_mrr) AS actual_mrr,
            cast(null AS double) AS forecast_lower,
            cast(null AS double) AS forecast_upper
        FROM main.fct_revenue_forecast_monthly
        WHERE scenario = $scenario AND segment = $segment
        GROUP BY month, segment, scenario
        ORDER BY month
        """
        return sql.strip(), {"scenario": scenario, "segment": segment}
    sql = """
    SELECT
        month,
        'All' AS segment,
        scenario,
        sum(forecast_mrr_total) AS forecast_mrr_total,
        sum(actual_mrr) AS actual_mrr,
        cast(null AS double) AS forecast_lower,
        cast(null AS double) AS forecast_upper
    FROM main.fct_revenue_forecast_monthly
    WHERE scenario = $scenario
    GROUP BY month, scenario
    ORDER BY month
    """
    return sql.strip(), {"scenario": scenario}


def get_arr_waterfall(month: str, scenario: str, segment: str) -> tuple[str, dict[str, Any]]:
    """
    One row from mart_arr_waterfall_monthly for (month, scenario). segment='All' aggregates across segments.
    Columns: starting_arr, new_arr, expansion_arr, contraction_arr, churn_arr, ending_arr, net_new_arr, nrr, grr.
    """
    if segment and segment != "All":
        sql = """
        SELECT
            month,
            segment,
            scenario,
            sum(starting_arr) AS starting_arr,
            sum(new_arr) AS new_arr,
            sum(expansion_arr) AS expansion_arr,
            sum(contraction_arr) AS contraction_arr,
            sum(churn_arr) AS churn_arr,
            sum(ending_arr) AS ending_arr,
            sum(net_new_arr) AS net_new_arr,
            (sum(starting_arr) + sum(expansion_arr) - sum(contraction_arr) - sum(churn_arr)) / nullif(sum(starting_arr), 0) AS nrr,
            (sum(starting_arr) - sum(contraction_arr) - sum(churn_arr)) / nullif(sum(starting_arr), 0) AS grr
        FROM main.mart_arr_waterfall_monthly
        WHERE month = $month AND scenario = $scenario AND segment = $segment
        GROUP BY month, segment, scenario
        """
        return sql.strip(), {"month": month, "scenario": scenario, "segment": segment}
    sql = """
    SELECT
        month,
        'All' AS segment,
        scenario,
        sum(starting_arr) AS starting_arr,
        sum(new_arr) AS new_arr,
        sum(expansion_arr) AS expansion_arr,
        sum(contraction_arr) AS contraction_arr,
        sum(churn_arr) AS churn_arr,
        sum(ending_arr) AS ending_arr,
        sum(net_new_arr) AS net_new_arr,
        (sum(starting_arr) + sum(expansion_arr) - sum(contraction_arr) - sum(churn_arr)) / nullif(sum(starting_arr), 0) AS nrr,
        (sum(starting_arr) - sum(contraction_arr) - sum(churn_arr)) / nullif(sum(starting_arr), 0) AS grr
    FROM main.mart_arr_waterfall_monthly
    WHERE month = $month AND scenario = $scenario
    GROUP BY month, scenario
    """
    return sql.strip(), {"month": month, "scenario": scenario}


def get_arr_reconciliation(month: str, scenario: str, segment: str) -> tuple[str, dict[str, Any]]:
    """
    Reconciliation check for (month, scenario, segment). ok_flag, diff. Caller handles missing table.
    """
    if segment and segment != "All":
        sql = """
        SELECT
            bool_and(arr_reconciliation_ok_flag) AS ok_flag,
            sum(arr_reconciliation_diff) AS diff
        FROM main.mart_arr_reconciliation_checks
        WHERE month = $month AND scenario = $scenario AND segment = $segment
        GROUP BY month, scenario, segment
        """
        params = {"month": month, "scenario": scenario, "segment": segment}
    else:
        sql = """
        SELECT
            bool_and(arr_reconciliation_ok_flag) AS ok_flag,
            sum(arr_reconciliation_diff) AS diff
        FROM main.mart_arr_reconciliation_checks
        WHERE month = $month AND scenario = $scenario
        GROUP BY month, scenario
        """
        params = {"month": month, "scenario": scenario}
    return sql.strip(), params


# Legacy placeholders (for other pages)
def sql_executive_forecast_summary() -> str:
    return "SELECT * FROM main.mart_executive_forecast_summary LIMIT 100"


def sql_arr_waterfall() -> str:
    return "SELECT * FROM main.mart_arr_waterfall_monthly LIMIT 100"


def sql_churn_risk_watchlist() -> str:
    return "SELECT * FROM main.mart_churn_risk_watchlist LIMIT 100"


def sql_ml_calibration_bins() -> str:
    return "SELECT * FROM main.mart_ml_calibration_bins LIMIT 100"
