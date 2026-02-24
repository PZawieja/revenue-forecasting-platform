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
            case when nullif(sum(starting_arr), 0) is null then null else coalesce((sum(starting_arr) + sum(expansion_arr) - sum(contraction_arr) - sum(churn_arr)) / nullif(sum(starting_arr), 0), 1) end AS nrr,
            case when nullif(sum(starting_arr), 0) is null then null else coalesce((sum(starting_arr) - sum(contraction_arr) - sum(churn_arr)) / nullif(sum(starting_arr), 0), 1) end AS grr
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
        case when nullif(sum(starting_arr), 0) is null then null else coalesce((sum(starting_arr) + sum(expansion_arr) - sum(contraction_arr) - sum(churn_arr)) / nullif(sum(starting_arr), 0), 1) end AS nrr,
        case when nullif(sum(starting_arr), 0) is null then null else coalesce((sum(starting_arr) - sum(contraction_arr) - sum(churn_arr)) / nullif(sum(starting_arr), 0), 1) end AS grr
    FROM main.mart_arr_waterfall_monthly
    WHERE month = $month AND scenario = $scenario
    GROUP BY month, scenario
    """
    return sql.strip(), {"month": month, "scenario": scenario}


def get_arr_waterfall_recent(scenario: str, segment: str, limit_months: int = 6) -> tuple[str, dict[str, Any]]:
    """
    Last N months that have non-zero ARR from mart_arr_waterfall_monthly (so we don't show future/empty months).
    """
    if segment and segment != "All":
        sql = """
        WITH agg AS (
            SELECT month, segment, scenario,
                sum(starting_arr) AS starting_arr, sum(ending_arr) AS ending_arr,
                sum(new_arr) AS new_arr, sum(expansion_arr) AS expansion_arr,
                sum(contraction_arr) AS contraction_arr, sum(churn_arr) AS churn_arr
            FROM main.mart_arr_waterfall_monthly
            WHERE scenario = $scenario AND segment = $segment
            GROUP BY month, segment, scenario
        )
        SELECT * FROM agg
        WHERE starting_arr > 0 OR ending_arr > 0
        ORDER BY month DESC
        LIMIT $limit_months
        """
        return sql.strip(), {"scenario": scenario, "segment": segment, "limit_months": limit_months}
    # Aggregate first, then filter to months with data (DuckDB: use subquery so HAVING applies to sums)
    sql = """
    WITH agg AS (
        SELECT month, 'All' AS segment, scenario,
            sum(starting_arr) AS starting_arr, sum(ending_arr) AS ending_arr,
            sum(new_arr) AS new_arr, sum(expansion_arr) AS expansion_arr,
            sum(contraction_arr) AS contraction_arr, sum(churn_arr) AS churn_arr
        FROM main.mart_arr_waterfall_monthly
        WHERE scenario = $scenario
        GROUP BY month, scenario
    )
    SELECT * FROM agg
    WHERE starting_arr > 0 OR ending_arr > 0
    ORDER BY month DESC
    LIMIT $limit_months
    """
    return sql.strip(), {"scenario": scenario, "limit_months": limit_months}


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


def get_churn_risk_watchlist(month: str, segment: str) -> tuple[str, dict[str, Any]]:
    """
    Top 20 by risk_rank from mart_churn_risk_watchlist for (month, segment).
    Joins dim_customer for customer_name. segment='All' returns all segments.
    """
    if segment and segment != "All":
        sql = """
        SELECT
            w.risk_rank,
            coalesce(c.customer_name, w.customer_id::varchar) AS customer_name,
            w.segment,
            w.months_to_renewal,
            w.current_arr,
            w.p_renew,
            w.health_score_1_10,
            w.slope_bucket,
            w.risk_reason
        FROM main.mart_churn_risk_watchlist w
        LEFT JOIN main.dim_customer c ON c.company_id = w.company_id AND c.customer_id = w.customer_id
        WHERE w.month = $month AND w.segment = $segment
        ORDER BY w.risk_rank
        LIMIT 20
        """
        return sql.strip(), {"month": month, "segment": segment}
    sql = """
    SELECT
        w.risk_rank,
        coalesce(c.customer_name, w.customer_id::varchar) AS customer_name,
        w.segment,
        w.months_to_renewal,
        w.current_arr,
        w.p_renew,
        w.health_score_1_10,
        w.slope_bucket,
        w.risk_reason
    FROM main.mart_churn_risk_watchlist w
    LEFT JOIN main.dim_customer c ON c.company_id = w.company_id AND c.customer_id = w.customer_id
    WHERE w.month = $month
    ORDER BY w.risk_rank
    LIMIT 20
    """
    return sql.strip(), {"month": month}


def get_top_arr_movers(month: str, segment: str) -> tuple[str, dict[str, Any]]:
    """
    Top 10 from mart_top_arr_movers for (month, segment). segment='All' aggregates (all segments).
    """
    if segment and segment != "All":
        sql = """
        SELECT
            customer_name,
            arr_delta,
            bridge_category,
            health_score_1_10,
            slope_bucket
        FROM main.mart_top_arr_movers
        WHERE month = $month AND segment = $segment
        ORDER BY rank
        LIMIT 10
        """
        return sql.strip(), {"month": month, "segment": segment}
    sql = """
    SELECT
        customer_name,
        arr_delta,
        bridge_category,
        health_score_1_10,
        slope_bucket
    FROM main.mart_top_arr_movers
    WHERE month = $month
    ORDER BY rank
    LIMIT 10
    """
    return sql.strip(), {"month": month}


def get_months_for_risk() -> tuple[str, dict[str, Any]]:
    """Distinct months available in mart_churn_risk_watchlist."""
    sql = """
    SELECT DISTINCT month
    FROM main.mart_churn_risk_watchlist
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


def get_model_selection() -> tuple[str, dict[str, Any]]:
    """Read ml_model_selection (dataset, preferred_model, selection_reason, scores if present). Caller handles missing table."""
    sql = """
    SELECT * FROM main.ml_model_selection ORDER BY dataset
    """
    return sql.strip(), {}


def get_latest_backtest_metrics(dataset: str) -> tuple[str, dict[str, Any]]:
    """
    Latest cutoff_month metrics from ml_renewal_backtest_metrics or ml_pipeline_backtest_metrics.
    Returns all columns (model_name, segment, auc, brier, logloss, etc.). Caller handles missing table.
    """
    table = "main.ml_renewal_backtest_metrics" if dataset == "renewals" else "main.ml_pipeline_backtest_metrics"
    sql = f"""
    SELECT * FROM {table}
    WHERE cutoff_month = (SELECT max(cutoff_month) FROM {table})
    ORDER BY model_name, segment
    """
    return sql.strip(), {}


def get_latest_calibration_bins(dataset: str, model_name: str) -> tuple[str, dict[str, Any]]:
    """
    Calibration bins 1..10 for latest cutoff_month from ml_calibration_bins. Caller handles missing table.
    """
    sql = """
    SELECT bin_id, p_pred_mean, y_true_rate, count
    FROM main.ml_calibration_bins
    WHERE dataset = $dataset AND model_name = $model_name
      AND cutoff_month = (SELECT max(cutoff_month) FROM main.ml_calibration_bins WHERE dataset = $dataset AND model_name = $model_name)
    ORDER BY bin_id
    """
    return sql.strip(), {"dataset": dataset, "model_name": model_name}


def sql_ml_calibration_bins() -> str:
    return "SELECT * FROM main.mart_ml_calibration_bins LIMIT 100"
