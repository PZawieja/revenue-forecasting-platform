-- Forecast coverage metrics for governance. Grain: company_id x month x segment x scenario.
-- Assumptions: (1) "Next 3 months" = report_month+1, +2, +3. (2) Pipeline and forecast in MRR; ratios use same units. (3) Renewal ARR at risk = current-month MRR for contracts ending in next 3 months, annualized. (4) Concentration and new_logo from actuals/waterfall; deterministic ordering.

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

forecast as (
    select
        company_id,
        month,
        scenario,
        segment,
        forecast_mrr_new_biz,
        forecast_mrr_renewal + forecast_mrr_new_biz + forecast_mrr_expansion as forecast_mrr_total
    from {{ ref('fct_revenue_forecast_monthly') }}
),

-- Pipeline coverage: sum(new_biz) and sum(forecast_total) for next 3 months; ratio = new_biz / total
next_3m_forecast as (
    select
        f.company_id,
        f.month,
        f.scenario,
        f.segment,
        sum(f.forecast_mrr_new_biz) as sum_new_biz_3m,
        sum(f.forecast_mrr_total) as sum_total_3m
    from forecast f
    inner join forecast f2
        on f2.company_id = f.company_id and f2.scenario = f.scenario and f2.segment = f.segment
        and f2.month > f.month and f2.month <= (f.month + interval '3 months')::date
    group by f.company_id, f.month, f.scenario, f.segment
),

pipeline_coverage as (
    select
        company_id,
        month,
        scenario,
        segment,
        sum_new_biz_3m / nullif(sum_total_3m, 0) as pipeline_coverage_ratio
    from next_3m_forecast
),

-- Renewal coverage: ARR up for renewal in next 3 months / total forecast ARR next 3 months
-- ARR at risk = 12 * sum(mrr) in report month for contracts with contract_end_month in [month+1, month+3]
stg as (
    select company_id, customer_id, product_id, contract_id, contract_end_month
    from {{ ref('stg_subscription_line_items') }}
),

line_item_month as (
    select
        lim.company_id,
        lim.customer_id,
        lim.product_id,
        lim.contract_id,
        lim.month,
        lim.mrr,
        s.contract_end_month
    from {{ ref('fct_subscription_line_item_monthly') }} lim
    inner join stg s
        on s.company_id = lim.company_id and s.customer_id = lim.customer_id
        and s.product_id = lim.product_id and s.contract_id = lim.contract_id
),

arr_up_for_renewal as (
    select
        lim.company_id,
        lim.month,
        c.segment,
        12.0 * sum(lim.mrr) as arr_up_for_renewal_3m
    from line_item_month lim
    inner join {{ ref('dim_customer') }} c on c.company_id = lim.company_id and c.customer_id = lim.customer_id
    where lim.contract_end_month > lim.month
      and lim.contract_end_month <= (lim.month + interval '3 months')::date
    group by lim.company_id, lim.month, c.segment
),

renewal_coverage as (
    select
        n.company_id,
        n.month,
        n.scenario,
        n.segment,
        coalesce(r.arr_up_for_renewal_3m, 0) / nullif(4.0 * n.sum_total_3m, 0) as renewal_coverage_ratio
    from next_3m_forecast n
    left join arr_up_for_renewal r on r.company_id = n.company_id and r.month = n.month and r.segment = n.segment
),

-- Concentration: top 5 customers ARR share per (company_id, month, segment); scenario-agnostic
customer_arr as (
    select
        lim.company_id,
        lim.month,
        c.segment,
        lim.customer_id,
        12.0 * sum(lim.mrr) as arr
    from {{ ref('fct_subscription_line_item_monthly') }} lim
    inner join {{ ref('dim_customer') }} c on c.company_id = lim.company_id and c.customer_id = lim.customer_id
    group by lim.company_id, lim.month, c.segment, lim.customer_id
),

segment_total as (
    select company_id, month, segment, sum(arr) as segment_arr
    from customer_arr
    group by company_id, month, segment
),

ranked as (
    select
        company_id,
        month,
        segment,
        customer_id,
        arr,
        row_number() over (partition by company_id, month, segment order by arr desc, customer_id) as rn
    from customer_arr
),

top5_sum as (
    select
        company_id,
        month,
        segment,
        sum(arr) as top5_arr
    from ranked
    where rn <= 5
    group by company_id, month, segment
),

concentration as (
    select
        t.company_id,
        t.month,
        t.segment,
        t.top5_arr / nullif(s.segment_arr, 0) as concentration_ratio_top5
    from top5_sum t
    inner join segment_total s on s.company_id = t.company_id and s.month = t.month and s.segment = t.segment
),

-- New logo share from waterfall: new_arr / ending_arr
waterfall as (
    select company_id, month, segment, scenario, new_arr, ending_arr
    from {{ ref('mart_arr_waterfall_monthly') }}
),

new_logo as (
    select
        company_id,
        month,
        segment,
        scenario,
        new_arr / nullif(ending_arr, 0) as new_logo_share
    from waterfall
),

joined as (
    select
        m.company_id,
        m.month,
        m.segment,
        m.scenario,
        p.pipeline_coverage_ratio,
        r.renewal_coverage_ratio,
        c.concentration_ratio_top5,
        n.new_logo_share
    from month_segment_scenario m
    left join pipeline_coverage p on p.company_id = m.company_id and p.month = m.month and p.scenario = m.scenario and p.segment = m.segment
    left join renewal_coverage r on r.company_id = m.company_id and r.month = m.month and r.scenario = m.scenario and r.segment = m.segment
    left join concentration c on c.company_id = m.company_id and c.month = m.month and c.segment = m.segment
    left join new_logo n on n.company_id = m.company_id and n.month = m.month and n.scenario = m.scenario and n.segment = m.segment
)

select
    company_id,
    month,
    segment,
    scenario,
    coalesce(pipeline_coverage_ratio, 0) as pipeline_coverage_ratio,
    coalesce(renewal_coverage_ratio, 0) as renewal_coverage_ratio,
    coalesce(concentration_ratio_top5, 0) as concentration_ratio_top5,
    new_logo_share
from joined
