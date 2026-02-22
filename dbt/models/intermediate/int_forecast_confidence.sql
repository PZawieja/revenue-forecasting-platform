-- Forecast confidence score by month x segment x scenario.
-- Inputs: fct_revenue_forecast_monthly, int_customer_health_monthly, customer-level forecast (renewal + expansion).
-- Risk indicators: % revenue from low-health customers, % from pipeline, top-5 concentration; combined into 0-100 score.
with forecast_monthly as (
    select
        company_id,
        month,
        scenario,
        segment,
        forecast_mrr_renewal,
        forecast_mrr_new_biz,
        forecast_mrr_expansion,
        forecast_mrr_total
    from {{ ref('fct_revenue_forecast_monthly') }}
),

customer_forecast as (
    select
        r.company_id,
        r.month,
        r.scenario,
        c.segment,
        r.customer_id,
        r.forecast_mrr as forecast_mrr
    from {{ ref('fct_forecast_component_renewals') }} r
    inner join {{ ref('dim_customer') }} c on r.company_id = c.company_id and r.customer_id = c.customer_id
    union all
    select
        e.company_id,
        e.month,
        e.scenario,
        c.segment,
        e.customer_id,
        e.forecast_mrr as forecast_mrr
    from {{ ref('fct_forecast_component_expansion') }} e
    inner join {{ ref('dim_customer') }} c on e.company_id = c.company_id and e.customer_id = c.customer_id
),

customer_forecast_agg as (
    select
        company_id,
        month,
        scenario,
        segment,
        customer_id,
        sum(forecast_mrr) as customer_forecast_mrr
    from customer_forecast
    group by company_id, month, scenario, segment, customer_id
),

with_health as (
    select
        a.company_id,
        a.month,
        a.scenario,
        a.segment,
        a.customer_id,
        a.customer_forecast_mrr,
        h.health_score_1_10
    from customer_forecast_agg a
    left join {{ ref('int_customer_health_monthly') }} h
        on h.company_id = a.company_id and h.customer_id = a.customer_id and h.month = a.month
),

health_risk_by_grain as (
    select
        w.company_id,
        w.month,
        w.scenario,
        w.segment,
        sum(case when w.health_score_1_10 <= 4 then w.customer_forecast_mrr else 0 end) as forecast_from_low_health,
        sum(w.customer_forecast_mrr) as total_customer_forecast
    from with_health w
    group by w.company_id, w.month, w.scenario, w.segment
),

ranked as (
    select
        company_id,
        month,
        scenario,
        segment,
        customer_forecast_mrr,
        sum(customer_forecast_mrr) over (partition by company_id, month, scenario, segment) as total_customer_forecast,
        row_number() over (partition by company_id, month, scenario, segment order by customer_forecast_mrr desc nulls last) as rn
    from customer_forecast_agg
),

concentration_by_grain as (
    select
        company_id,
        month,
        scenario,
        segment,
        max(total_customer_forecast) as total_customer_forecast,
        sum(case when rn <= 5 then customer_forecast_mrr else 0 end) as top5_forecast
    from ranked
    group by company_id, month, scenario, segment
),

risks as (
    select
        f.company_id,
        f.month,
        f.segment,
        f.scenario,
        f.forecast_mrr_total,
        f.forecast_mrr_new_biz,
        coalesce(h.forecast_from_low_health, 0) as forecast_from_low_health,
        coalesce(c.total_customer_forecast, 0) as total_customer_forecast,
        coalesce(c.top5_forecast, 0) as top5_forecast
    from forecast_monthly f
    left join health_risk_by_grain h
        on f.company_id = h.company_id and f.month = h.month and f.scenario = h.scenario and f.segment = h.segment
    left join concentration_by_grain c
        on f.company_id = c.company_id and f.month = c.month and f.scenario = c.scenario and f.segment = c.segment
),

normalized as (
    select
        company_id,
        month,
        segment,
        scenario,
        -- Raw risk metrics (risk_breakdown)
        case
            when forecast_mrr_total > 0 then least(1.0, forecast_from_low_health / forecast_mrr_total)
            else 0.0
        end as pct_revenue_low_health,
        case
            when forecast_mrr_total > 0 then least(1.0, forecast_mrr_new_biz / forecast_mrr_total)
            else 0.0
        end as pct_forecast_from_pipeline,
        case
            when total_customer_forecast > 0 then least(1.0, top5_forecast / total_customer_forecast)
            else 0.0
        end as top5_concentration_pct,
        -- Normalized 0-1 (high = lower confidence)
        case
            when forecast_mrr_total > 0 then least(1.0, forecast_from_low_health / forecast_mrr_total)
            else 0.0
        end as health_risk_norm,
        case
            when forecast_mrr_total > 0 then least(1.0, forecast_mrr_new_biz / forecast_mrr_total)
            else 0.0
        end as pipeline_risk_norm,
        case
            when total_customer_forecast > 0 then least(1.0, top5_forecast / total_customer_forecast)
            else 0.0
        end as concentration_risk_norm
    from risks
)

select
    company_id,
    month,
    segment,
    scenario,
    round(100.0 * (1.0 - (
        0.4 * health_risk_norm
        + 0.35 * pipeline_risk_norm
        + 0.25 * concentration_risk_norm
    )))::integer as confidence_score_0_100,
    pct_revenue_low_health,
    pct_forecast_from_pipeline,
    top5_concentration_pct
from normalized
