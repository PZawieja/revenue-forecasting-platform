# Custom generic tests (data contract)

- **accepted_range:** Validates a numeric column is between `min_value` and `max_value` (inclusive). Nulls are ignored. Use in schema.yml with `min_value` and `max_value` under the test name.

**Limitations:**

- **Required columns present:** Not implemented as a generic test; dbt model compilation fails if referenced columns are missing. No separate test needed.
- **Scenario completeness:** Handled by the singular test `tests/assert_fct_revenue_forecast_monthly_scenario_completeness.sql` (each month/segment has exactly 3 scenarios).
- **No future leakage:** Handled by `tests/assert_ml_features_renewals_no_future_leakage.sql` for `ml_features_renewals` (renewal_month must be ≤ max(month) in actuals − 1 month). Pipeline labels are defined as “later snapshot only” in the model; no separate leakage test for pipeline.
