-- Forecast explainability: month-over-month forecast change by driver (renewal, pipeline, expansion, residual).
-- Grain: company_id x month x segment x scenario.
-- Deterministic approximation: driver deltas = component MRR changes (renewal, new_biz, expansion) vs prior month;
-- residual_delta = forecast_delta - sum of driver deltas. top_driver = driver with largest abs(delta).
-- Prior month missing (e.g. first month) => prior set to 0. Explainability layer only; not a causal decomposition.

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
),

with_prior as (
    select
        curr.company_id,
        curr.month,
        curr.segment,
        curr.scenario,
        curr.forecast_mrr_total,
        coalesce(prev.forecast_mrr_total, 0) as forecast_mrr_total_prior_month,
        curr.forecast_mrr_renewal,
        coalesce(prev.forecast_mrr_renewal, 0) as forecast_mrr_renewal_prior,
        curr.forecast_mrr_new_biz,
        coalesce(prev.forecast_mrr_new_biz, 0) as forecast_mrr_new_biz_prior,
        curr.forecast_mrr_expansion,
        coalesce(prev.forecast_mrr_expansion, 0) as forecast_mrr_expansion_prior
    from fct curr
    left join fct prev
        on prev.company_id = curr.company_id
        and prev.segment = curr.segment
        and prev.scenario = curr.scenario
        and prev.month = (curr.month - interval '1 month')::date
),

deltas as (
    select
        company_id,
        month,
        segment,
        scenario,
        forecast_mrr_total,
        forecast_mrr_total_prior_month,
        forecast_mrr_total - forecast_mrr_total_prior_month as forecast_delta,
        (forecast_mrr_renewal - forecast_mrr_renewal_prior) as renewal_driver_delta,
        (forecast_mrr_new_biz - forecast_mrr_new_biz_prior) as pipeline_driver_delta,
        (forecast_mrr_expansion - forecast_mrr_expansion_prior) as expansion_driver_delta
    from with_prior
),

with_residual as (
    select
        *,
        forecast_delta - (renewal_driver_delta + pipeline_driver_delta + expansion_driver_delta) as residual_delta
    from deltas
),

with_top_driver as (
    select
        *,
        case
            when abs(residual_delta) >= abs(renewal_driver_delta) and abs(residual_delta) >= abs(pipeline_driver_delta) and abs(residual_delta) >= abs(expansion_driver_delta) then 'residual'
            when abs(renewal_driver_delta) >= abs(pipeline_driver_delta) and abs(renewal_driver_delta) >= abs(expansion_driver_delta) then 'renewal'
            when abs(pipeline_driver_delta) >= abs(expansion_driver_delta) then 'pipeline'
            else 'expansion'
        end as top_driver
    from with_residual
)

select
    company_id,
    month,
    segment,
    scenario,
    forecast_mrr_total,
    forecast_mrr_total_prior_month,
    forecast_delta,
    renewal_driver_delta,
    pipeline_driver_delta,
    expansion_driver_delta,
    residual_delta,
    top_driver,
    case
        when top_driver = 'renewal' then abs(renewal_driver_delta) / nullif(abs(forecast_delta), 0)
        when top_driver = 'pipeline' then abs(pipeline_driver_delta) / nullif(abs(forecast_delta), 0)
        when top_driver = 'expansion' then abs(expansion_driver_delta) / nullif(abs(forecast_delta), 0)
        when top_driver = 'residual' then abs(residual_delta) / nullif(abs(forecast_delta), 0)
        else null
    end as top_driver_share_pct
from with_top_driver
