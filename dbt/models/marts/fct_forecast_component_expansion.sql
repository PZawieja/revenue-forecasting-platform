with current_mrr as (
    select
        company_id,
        customer_id,
        month,
        sum(mrr) as current_mrr
    from {{ ref('fct_subscription_line_item_monthly') }}
    group by company_id, customer_id, month
),

health as (
    select
        company_id,
        customer_id,
        month,
        segment,
        segment_group,
        trailing_3m_slope_bucket as slope_bucket
    from {{ ref('int_customer_health_monthly') }}
),

with_uplift as (
    select
        m.company_id,
        m.customer_id,
        m.month,
        m.current_mrr,
        h.segment_group,
        h.slope_bucket,
        case
            when h.slope_bucket = 'growing' then sc.expansion_growing_uplift_pct
            when h.slope_bucket = 'flat' then sc.expansion_flat_uplift_pct
            when h.slope_bucket = 'declining' then sc.expansion_declining_uplift_pct
            else 0
        end as base_uplift_pct
    from current_mrr m
    inner join health h on m.company_id = h.company_id and m.customer_id = h.customer_id and m.month = h.month
    inner join {{ ref('segment_config') }} sc on sc.company_id = h.company_id and sc.segment = h.segment
),

scenarios as (
    select
        u.company_id,
        u.month,
        u.customer_id,
        s.scenario,
        'expansion' as component,
        u.current_mrr * greatest(-0.02, u.base_uplift_pct + coalesce(s.expansion_adjustment, 0)) as forecast_mrr
    from with_uplift u
    cross join {{ ref('scenario_config') }} s
)
select * from scenarios
