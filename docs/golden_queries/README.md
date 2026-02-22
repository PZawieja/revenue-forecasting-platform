# Golden queries

Reference SQL snippets that answer common executive questions against the canonical marts. Safe to copy-paste into DuckDB (or your SQL client) after running dbt and optional ML. Use from repo root with the same DuckDB warehouse the app uses (`warehouse/revenue_forecasting.duckdb`).

- **No hardcoded dates:** Queries use `MAX(month)` or equivalent so they stay valid as data is refreshed.
- **Canonical marts:** `mart_executive_forecast_summary`, `mart_arr_waterfall_monthly`, `mart_churn_risk_watchlist`, `mart_top_arr_movers`, `mart_forecast_coverage_metrics`, `ml_model_selection`, and `mart_ml_*` where present.
- **CTEs:** Queries are structured with clear CTEs for readability and reuse.
