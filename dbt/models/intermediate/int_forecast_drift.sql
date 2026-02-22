-- Forecast drift monitoring: compare current forecast vs prior snapshot for the same (month, scenario, segment).
-- Requires the forecast table to be snapshot historically; prior snapshot is read from
-- fct_revenue_forecast_monthly_prior_snapshot (placeholder until dbt snapshot or historical load is implemented).
-- Grain: month x scenario x segment. Only outputs rows where a prior snapshot exists for that grain.
with current_forecast as (
    select
        company_id,
        month,
        scenario,
        segment,
        coalesce(forecast_mrr_renewal + forecast_mrr_new_biz + forecast_mrr_expansion, 0) as forecast_current
    from {{ ref('fct_revenue_forecast_monthly') }}
),

previous_forecast as (
    select
        company_id,
        month,
        scenario,
        segment,
        forecast_mrr_total as forecast_previous
    from {{ ref('fct_revenue_forecast_monthly_prior_snapshot') }}
),

joined as (
    select
        c.company_id,
        c.month,
        c.segment,
        c.scenario,
        c.forecast_current,
        p.forecast_previous,
        c.forecast_current - p.forecast_previous as drift,
        case
            when coalesce(p.forecast_previous, 0) <> 0 then (c.forecast_current - p.forecast_previous) / p.forecast_previous
            else null
        end as drift_pct
    from current_forecast c
    inner join previous_forecast p
        on p.company_id = c.company_id and p.month = c.month and p.scenario = c.scenario and p.segment = c.segment
)

select
    company_id,
    month,
    segment,
    scenario,
    forecast_current,
    forecast_previous,
    drift,
    drift_pct,
    (abs(coalesce(drift_pct, 0)) > 0.10) as drift_flag
from joined
