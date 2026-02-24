-- ARR Waterfall (ARR bridge): month-over-month ARR movement by segment and scenario.
-- Grain: company_id x month x segment x scenario.
-- ARR = 12 * MRR. Base scenario uses actuals (fct_subscription_line_item_monthly);
-- upside/downside use fct_revenue_forecast_monthly converted to ARR.
-- Requires int_month_spine for month coverage; scenario completeness (base, upside, downside).

with spine as (
    select month from {{ ref('int_month_spine') }}
),

scenarios as (
    select * from (values ('base'), ('upside'), ('downside')) as t(scenario)
),

company_segments as (
    select distinct company_id, segment from {{ ref('dim_customer') }}
),

month_segment_scenario as (
    select s.month, cs.company_id, cs.segment, sc.scenario
    from spine s
    cross join company_segments cs
    cross join scenarios sc
),

-- Customer-level ARR by month (actuals): 12 * MRR from subscription line items
customer_arr_monthly as (
    select
        lim.company_id,
        lim.customer_id,
        c.segment,
        lim.month,
        12.0 * sum(lim.mrr) as arr
    from {{ ref('fct_subscription_line_item_monthly') }} lim
    inner join {{ ref('dim_customer') }} c on lim.company_id = c.company_id and lim.customer_id = c.customer_id
    group by lim.company_id, lim.customer_id, c.segment, lim.month
),

-- Prior month for each (company, customer, segment, month)
with_prior as (
    select
        curr.company_id,
        curr.customer_id,
        curr.segment,
        curr.month,
        curr.arr as arr_current,
        prev.arr as arr_prior
    from customer_arr_monthly curr
    left join customer_arr_monthly prev
        on prev.company_id = curr.company_id
        and prev.customer_id = curr.customer_id
        and prev.segment = curr.segment
        and prev.month = (curr.month - interval '1 month')::date
),

-- Classify: NEW (0 prior, >0 current), CHURN (>0 prior, 0 current), EXPANSION (both >0, delta>0), CONTRACTION (both >0, delta<0)
classified as (
    select
        company_id,
        segment,
        month,
        coalesce(arr_prior, 0) as arr_prior,
        coalesce(arr_current, 0) as arr_current,
        case when coalesce(arr_prior, 0) = 0 and coalesce(arr_current, 0) > 0 then coalesce(arr_current, 0) else 0 end as new_arr,
        case when coalesce(arr_prior, 0) > 0 and coalesce(arr_current, 0) = 0 then coalesce(arr_prior, 0) else 0 end as churn_arr,
        case when coalesce(arr_prior, 0) > 0 and coalesce(arr_current, 0) > 0 and (coalesce(arr_current, 0) - coalesce(arr_prior, 0)) > 0 then (arr_current - arr_prior) else 0 end as expansion_arr,
        case when coalesce(arr_prior, 0) > 0 and coalesce(arr_current, 0) > 0 and (coalesce(arr_current, 0) - coalesce(arr_prior, 0)) < 0 then (arr_prior - arr_current) else 0 end as contraction_arr
    from with_prior
),

-- Roll up base scenario to company_id x month x segment
base_waterfall as (
    select
        company_id,
        month,
        segment,
        'base' as scenario,
        sum(arr_prior) as starting_arr,
        sum(new_arr) as new_arr,
        sum(expansion_arr) as expansion_arr,
        sum(contraction_arr) as contraction_arr,
        sum(churn_arr) as churn_arr,
        sum(arr_current) as ending_arr
    from classified
    group by company_id, month, segment
),

-- Forecast scenarios: starting_arr = prior month forecast ARR, ending_arr = current month; components derived
forecast_with_prior as (
    select
        curr.company_id,
        curr.month,
        curr.segment,
        curr.scenario,
        12.0 * coalesce(prev.forecast_mrr_total, 0) as starting_arr,
        12.0 * curr.forecast_mrr_total as ending_arr
    from {{ ref('fct_revenue_forecast_monthly') }} curr
    left join {{ ref('fct_revenue_forecast_monthly') }} prev
        on prev.company_id = curr.company_id
        and prev.segment = curr.segment
        and prev.scenario = curr.scenario
        and prev.month = (curr.month - interval '1 month')::date
    where curr.scenario in ('upside', 'downside')
),

-- Base (actuals) + upside/downside (forecast). For forecast, net = ending - starting; use new_arr (growth) or churn_arr (decline) only so reconciliation holds.
all_waterfall as (
    select company_id, month, segment, scenario, starting_arr, new_arr, expansion_arr, contraction_arr, churn_arr, ending_arr
    from base_waterfall
    union all
    select
        company_id,
        month,
        segment,
        scenario,
        coalesce(starting_arr, 0) as starting_arr,
        greatest(0, ending_arr - coalesce(starting_arr, 0)) as new_arr,
        0.0 as expansion_arr,
        0.0 as contraction_arr,
        greatest(0, coalesce(starting_arr, 0) - ending_arr) as churn_arr,
        ending_arr
    from forecast_with_prior
),

with_derived as (
    select
        company_id,
        month,
        segment,
        scenario,
        starting_arr,
        new_arr,
        expansion_arr,
        contraction_arr,
        churn_arr,
        ending_arr,
        new_arr + expansion_arr - contraction_arr - churn_arr as net_new_arr,
        case when nullif(starting_arr, 0) is null then null else coalesce((starting_arr + expansion_arr - contraction_arr - churn_arr) / nullif(starting_arr, 0), 1) end as nrr,
        case when nullif(starting_arr, 0) is null then null else coalesce((starting_arr - contraction_arr - churn_arr) / nullif(starting_arr, 0), 1) end as grr
    from all_waterfall
)

select
    m.company_id,
    m.month,
    m.segment,
    m.scenario,
    coalesce(w.starting_arr, 0) as starting_arr,
    coalesce(w.new_arr, 0) as new_arr,
    coalesce(w.expansion_arr, 0) as expansion_arr,
    coalesce(w.contraction_arr, 0) as contraction_arr,
    coalesce(w.churn_arr, 0) as churn_arr,
    coalesce(w.ending_arr, 0) as ending_arr,
    coalesce(w.net_new_arr, 0) as net_new_arr,
    w.nrr,
    w.grr
from month_segment_scenario m
left join with_derived w
    on w.company_id = m.company_id and w.month = m.month and w.segment = m.segment and w.scenario = m.scenario
