with forecast as (
    select
        company_id,
        month,
        scenario,
        segment,
        forecast_mrr_total,
        actual_mrr
    from {{ ref('fct_revenue_forecast_monthly') }}
),

with_errors as (
    select
        company_id,
        month,
        scenario,
        segment,
        forecast_mrr_total,
        actual_mrr,
        forecast_mrr_total - actual_mrr as error,
        abs(forecast_mrr_total - actual_mrr) as abs_error,
        case
            when actual_mrr = 0 then null
            else abs(forecast_mrr_total - actual_mrr) / actual_mrr
        end as ape
    from forecast
),

with_wape as (
    select
        company_id,
        month,
        scenario,
        segment,
        forecast_mrr_total,
        actual_mrr,
        coalesce(error, 0) as error,
        coalesce(abs_error, 0) as abs_error,
        ape,
        sum(coalesce(abs_error, 0)) over (
            partition by company_id, scenario, segment
            order by month
            rows between 5 preceding and current row
        ) as sum_abs_error_6m,
        sum(actual_mrr) over (
            partition by company_id, scenario, segment
            order by month
            rows between 5 preceding and current row
        ) as sum_actual_mrr_6m
    from with_errors
)

select
    company_id,
    month,
    scenario,
    segment,
    coalesce(forecast_mrr_total, 0) as forecast_mrr_total,
    coalesce(actual_mrr, 0) as actual_mrr,
    error,
    abs_error,
    ape,
    case
        when coalesce(sum_actual_mrr_6m, 0) = 0 then null
        else sum_abs_error_6m / sum_actual_mrr_6m
    end as wape_6m
from with_wape
