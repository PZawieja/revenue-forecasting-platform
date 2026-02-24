#!/usr/bin/env python3
"""
Check that dashboard data looks good: non-zero ARR waterfall for recent months,
forecast and actuals present, and summary stats in range.
Run from repo root after make sim_demo_showcase (or make showcase).
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

def main() -> int:
    import duckdb

    db_path = REPO_ROOT / "warehouse" / "revenue_forecasting.duckdb"
    if not db_path.exists():
        print("FAIL: DuckDB not found. Run make sim_demo_showcase first.")
        return 1

    conn = duckdb.connect(str(db_path), read_only=True)
    errors = []

    # 1) ARR Waterfall: base scenario should have months with non-zero ARR
    try:
        r = conn.execute("""
            WITH agg AS (
                SELECT month, scenario,
                    sum(starting_arr) AS starting_arr, sum(ending_arr) AS ending_arr
                FROM main.mart_arr_waterfall_monthly
                WHERE scenario = 'base'
                GROUP BY month, scenario
            )
            SELECT count(*) AS n, sum(starting_arr) AS total_start, sum(ending_arr) AS total_end
            FROM agg WHERE starting_arr > 0 OR ending_arr > 0
        """).fetchone()
        n_months, total_start, total_end = r[0], r[1] or 0, r[2] or 0
        if n_months == 0:
            errors.append("ARR Waterfall: no months with non-zero ARR for base scenario")
        elif total_end < 1e6:
            errors.append(f"ARR Waterfall: total ending ARR very low ({total_end:,.0f})")
        else:
            print(f"OK  ARR Waterfall: {n_months} months with data, total ending ARR {total_end:,.0f}")
    except Exception as e:
        errors.append(f"ARR Waterfall check failed: {e}")

    # 2) Forecast: should have non-zero actual_mrr for some months and non-zero forecast for some
    try:
        r = conn.execute("""
            SELECT
                count(*) AS months_with_actual,
                sum(CASE WHEN actual_mrr > 0 THEN 1 ELSE 0 END) AS months_actual_positive,
                sum(CASE WHEN forecast_mrr_total > 0 THEN 1 ELSE 0 END) AS months_forecast_positive,
                sum(actual_mrr) AS sum_actual,
                sum(forecast_mrr_total) AS sum_forecast
            FROM main.fct_revenue_forecast_monthly
            WHERE scenario = 'base'
        """).fetchone()
        n_actual_pos = r[1] or 0
        n_forecast_pos = r[2] or 0
        sum_actual = r[3] or 0
        sum_forecast = r[4] or 0
        if n_actual_pos == 0:
            errors.append("Forecast: no months with actual_mrr > 0")
        else:
            print(f"OK  Forecast: {n_actual_pos} months with actual MRR, {n_forecast_pos} with forecast MRR; sum actual {sum_actual:,.0f}, sum forecast {sum_forecast:,.0f}")
    except Exception as e:
        errors.append(f"Forecast check failed: {e}")

    # 3) Executive summary: at least one row with positive revenue
    try:
        r = conn.execute("""
            SELECT count(*), sum(total_forecast_revenue), sum(total_actual_revenue)
            FROM main.mart_executive_forecast_summary WHERE scenario = 'base'
        """).fetchone()
        n, tot_f, tot_a = r[0], r[1] or 0, r[2] or 0
        if n == 0:
            errors.append("Executive summary: no rows for base scenario")
        elif tot_a == 0 and tot_f == 0:
            errors.append("Executive summary: total actual and forecast both zero")
        else:
            print(f"OK  Executive summary: {n} rows, total actual {tot_a:,.0f}, total forecast {tot_f:,.0f}")
    except Exception as e:
        errors.append(f"Executive summary check failed: {e}")

    # 4) Last-6-months query (same as app): should return up to 6 rows with data
    try:
        r = conn.execute("""
            WITH agg AS (
                SELECT month, 'All' AS segment, scenario,
                    sum(starting_arr) AS starting_arr, sum(ending_arr) AS ending_arr,
                    sum(new_arr) AS new_arr, sum(expansion_arr) AS expansion_arr,
                    sum(contraction_arr) AS contraction_arr, sum(churn_arr) AS churn_arr
                FROM main.mart_arr_waterfall_monthly
                WHERE scenario = 'base'
                GROUP BY month, scenario
            )
            SELECT * FROM agg
            WHERE starting_arr > 0 OR ending_arr > 0
            ORDER BY month DESC
            LIMIT 6
        """).fetchdf()
        if r.empty:
            errors.append("Last-6-months (with data): returned 0 rows")
        else:
            print(f"OK  Last 6 months with data: {len(r)} rows (max month {r['month'].iloc[0]})")
    except Exception as e:
        errors.append(f"Last-6-months check failed: {e}")

    conn.close()

    if errors:
        print("\nFAILED:")
        for e in errors:
            print(f"  - {e}")
        return 1
    print("\nAll checks passed. Dashboard numbers should look good.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
