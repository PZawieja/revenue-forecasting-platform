-- Placeholder for prior forecast snapshot. Do NOT use in production until replaced.
-- This model exists only so int_forecast_drift can reference it. Replace with a model that
-- reads from a snapshot of fct_revenue_forecast_monthly (e.g. dbt snapshot or historical load)
-- and returns one row per (month, scenario, segment) for the "prior" snapshot (e.g. latest snapshot_date < today).
-- Until then, returns zero rows so drift monitoring has nothing to compare and int_forecast_drift returns no rows.
select
    company_id,
    month,
    scenario,
    segment,
    forecast_mrr_renewal + forecast_mrr_new_biz + forecast_mrr_expansion as forecast_mrr_total
from {{ ref('fct_revenue_forecast_monthly') }}
where 1 = 0
