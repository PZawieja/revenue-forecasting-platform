-- Prediction intervals around point forecast. Backtest-derived (historical WAPE), not probabilistic simulation.
-- Inputs: fct_revenue_forecast_monthly, fct_forecast_backtest_metrics.
-- volatility_factor = trailing 6-month WAPE per segment+scenario; bounds = forecast * (1 ± volatility_factor).
with forecast as (
    select
        company_id,
        month,
        segment,
        scenario,
        forecast_mrr_total,
        actual_mrr
    from {{ ref('fct_revenue_forecast_monthly') }}
),

backtest_wape as (
    select
        company_id,
        month,
        scenario,
        segment,
        wape_6m
    from {{ ref('fct_forecast_backtest_metrics') }}
),

latest_wape as (
    select company_id, scenario, segment, wape_6m
    from (
        select
            company_id,
            scenario,
            segment,
            wape_6m,
            row_number() over (partition by company_id, scenario, segment order by month desc) as rn
        from backtest_wape
        where wape_6m is not null
    ) t
    where rn = 1
),

wape_used as (
    select
        f.company_id,
        f.month,
        f.segment,
        f.scenario,
        coalesce(b.wape_6m, lw.wape_6m, 0) as volatility_factor
    from forecast f
    left join backtest_wape b
        on b.company_id = f.company_id and b.month = f.month and b.scenario = f.scenario and b.segment = f.segment
    left join latest_wape lw on lw.company_id = f.company_id and lw.scenario = f.scenario and lw.segment = f.segment
),

with_bounds as (
    select
        f.company_id,
        f.month,
        f.segment,
        f.scenario,
        f.forecast_mrr_total,
        w.volatility_factor,
        greatest(0, f.forecast_mrr_total * (1 - w.volatility_factor)) as forecast_lower,
        f.forecast_mrr_total * (1 + w.volatility_factor) as forecast_upper,
        f.actual_mrr
    from forecast f
    inner join wape_used w
        on w.company_id = f.company_id and w.month = f.month and w.segment = f.segment and w.scenario = f.scenario
)

select
    company_id,
    month,
    segment,
    scenario,
    forecast_mrr_total,
    forecast_lower,
    forecast_upper,
    actual_mrr
from with_bounds
