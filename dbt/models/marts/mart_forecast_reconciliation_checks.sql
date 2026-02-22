-- Forecast reconciliation (trust/audit): validate forecast_mrr_total = sum of components at segment/month/scenario.
-- Grain: company_id x month x segment x scenario.
-- Fails audit when forecast_reconciliation_ok_flag is false (abs(diff) > 0.01).

with fct as (
    select
        company_id,
        month,
        segment,
        scenario,
        forecast_mrr_renewal,
        forecast_mrr_new_biz,
        forecast_mrr_expansion,
        forecast_mrr_renewal + forecast_mrr_new_biz + forecast_mrr_expansion as forecast_mrr_total
    from {{ ref('fct_revenue_forecast_monthly') }}
)

select
    company_id,
    month,
    segment,
    scenario,
    forecast_mrr_total,
    forecast_mrr_renewal + forecast_mrr_new_biz + forecast_mrr_expansion as component_sum_mrr,
    forecast_mrr_total - (forecast_mrr_renewal + forecast_mrr_new_biz + forecast_mrr_expansion) as forecast_component_diff,
    abs(forecast_mrr_total - (forecast_mrr_renewal + forecast_mrr_new_biz + forecast_mrr_expansion)) <= 0.01 as forecast_reconciliation_ok_flag
from fct
