with spine as (
    select month from {{ ref('int_month_spine') }}
),

scenarios as (
    select * from (values ('base'), ('upside'), ('downside')) as t(scenario)
),

company_segments as (
    select distinct company_id, segment from {{ ref('dim_customer') }}
),

month_scenario_segment as (
    select
        s.month,
        sc.scenario,
        cs.company_id,
        cs.segment
    from spine s
    cross join scenarios sc
    cross join company_segments cs
),

renewal_agg as (
    select
        r.company_id,
        r.month,
        r.scenario,
        c.segment,
        sum(r.forecast_mrr) as forecast_mrr_renewal
    from {{ ref('fct_forecast_component_renewals') }} r
    inner join {{ ref('dim_customer') }} c on r.company_id = c.company_id and r.customer_id = c.customer_id
    group by r.company_id, r.month, r.scenario, c.segment
),

new_biz_segment as (
    select company_id, opportunity_id, scenario, segment
    from {{ ref('int_pipeline_weighted_value') }}
    where opportunity_type = 'new_biz'
    group by company_id, opportunity_id, scenario, segment
),

new_biz_agg as (
    select
        n.company_id,
        n.month,
        n.scenario,
        ns.segment,
        sum(n.forecast_mrr) as forecast_mrr_new_biz
    from {{ ref('fct_forecast_component_new_biz') }} n
    inner join new_biz_segment ns on n.company_id = ns.company_id and n.opportunity_id = ns.opportunity_id and n.scenario = ns.scenario
    group by n.company_id, n.month, n.scenario, ns.segment
),

expansion_agg as (
    select
        e.company_id,
        e.month,
        e.scenario,
        c.segment,
        sum(e.forecast_mrr) as forecast_mrr_expansion
    from {{ ref('fct_forecast_component_expansion') }} e
    inner join {{ ref('dim_customer') }} c on e.company_id = c.company_id and e.customer_id = c.customer_id
    group by e.company_id, e.month, e.scenario, c.segment
),

actuals_agg as (
    select company_id, month, segment, sum(mrr) as actual_mrr
    from {{ ref('fct_revenue_actuals_monthly') }}
    group by company_id, month, segment
),

forecast_components as (
    select
        m.company_id,
        m.month,
        m.scenario,
        m.segment,
        coalesce(r.forecast_mrr_renewal, 0) as forecast_mrr_renewal,
        coalesce(n.forecast_mrr_new_biz, 0) as forecast_mrr_new_biz,
        coalesce(e.forecast_mrr_expansion, 0) as forecast_mrr_expansion
    from month_scenario_segment m
    left join renewal_agg r on m.company_id = r.company_id and m.month = r.month and m.scenario = r.scenario and m.segment = r.segment
    left join new_biz_agg n on m.company_id = n.company_id and m.month = n.month and m.scenario = n.scenario and m.segment = n.segment
    left join expansion_agg e on m.company_id = e.company_id and m.month = e.month and m.scenario = e.scenario and m.segment = e.segment
)

select
    f.company_id,
    f.month,
    f.scenario,
    f.segment,
    f.forecast_mrr_renewal,
    f.forecast_mrr_new_biz,
    f.forecast_mrr_expansion,
    f.forecast_mrr_renewal + f.forecast_mrr_new_biz + f.forecast_mrr_expansion as forecast_mrr_total,
    coalesce(a.actual_mrr, 0) as actual_mrr
from forecast_components f
left join actuals_agg a on f.company_id = a.company_id and f.month = a.month and f.segment = a.segment
