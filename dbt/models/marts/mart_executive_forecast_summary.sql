-- Executive summary mart: month x scenario, aggregated across segments. For presentation use.
-- All numeric nulls default to 0 unless otherwise documented.
with forecast_agg as (
    select
        company_id,
        month,
        scenario,
        coalesce(sum(forecast_mrr_renewal + forecast_mrr_new_biz + forecast_mrr_expansion), 0) as total_forecast_revenue,
        coalesce(sum(actual_mrr), 0) as total_actual_revenue,
        coalesce(sum(forecast_mrr_renewal), 0) as total_renewal_component,
        coalesce(sum(forecast_mrr_new_biz), 0) as total_new_biz_component,
        coalesce(sum(forecast_mrr_expansion), 0) as total_expansion_component
    from {{ ref('fct_revenue_forecast_monthly') }}
    group by company_id, month, scenario
),

intervals_agg as (
    select
        company_id,
        month,
        scenario,
        coalesce(sum(forecast_lower), 0) as forecast_lower,
        coalesce(sum(forecast_upper), 0) as forecast_upper
    from {{ ref('fct_revenue_forecast_with_intervals') }}
    group by company_id, month, scenario
),

confidence_agg as (
    select
        company_id,
        month,
        scenario,
        avg(confidence_score_0_100) as avg_confidence_score
    from {{ ref('int_forecast_confidence') }}
    group by company_id, month, scenario
),

joined as (
    select
        f.company_id,
        f.month,
        f.scenario,
        f.total_forecast_revenue,
        f.total_actual_revenue,
        f.total_forecast_revenue - f.total_actual_revenue as forecast_error,
        case
            when f.total_actual_revenue > 0 then (f.total_forecast_revenue - f.total_actual_revenue) / f.total_actual_revenue
            else null
        end as forecast_error_pct,
        f.total_renewal_component,
        f.total_new_biz_component,
        f.total_expansion_component,
        i.forecast_lower,
        i.forecast_upper,
        c.avg_confidence_score
    from forecast_agg f
    left join intervals_agg i on i.company_id = f.company_id and i.month = f.month and i.scenario = f.scenario
    left join confidence_agg c on c.company_id = f.company_id and c.month = f.month and c.scenario = f.scenario
),

with_growth as (
    select
        company_id,
        month,
        scenario,
        total_forecast_revenue,
        total_actual_revenue,
        forecast_error,
        forecast_error_pct,
        total_renewal_component,
        total_new_biz_component,
        total_expansion_component,
        coalesce(avg_confidence_score, 0) as avg_confidence_score,
        forecast_lower,
        forecast_upper,
        lag(total_actual_revenue) over (partition by company_id, scenario order by month) as prev_month_actual,
        lag(total_actual_revenue, 12) over (partition by company_id, scenario order by month) as same_month_prior_year_actual
    from joined
)

select
    company_id,
    month,
    scenario,
    total_forecast_revenue,
    total_actual_revenue,
    forecast_error,
    coalesce(forecast_error_pct, 0) as forecast_error_pct,
    total_renewal_component,
    total_new_biz_component,
    total_expansion_component,
    case
        when coalesce(prev_month_actual, 0) > 0 then (total_actual_revenue - prev_month_actual) / prev_month_actual
        else 0
    end as revenue_growth_mom,
    case
        when coalesce(same_month_prior_year_actual, 0) > 0 then (total_actual_revenue - same_month_prior_year_actual) / same_month_prior_year_actual
        else 0
    end as revenue_growth_yoy,
    avg_confidence_score,
    forecast_lower,
    forecast_upper
from with_growth
