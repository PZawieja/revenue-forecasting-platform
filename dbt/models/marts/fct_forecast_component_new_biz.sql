with slippage as (
    select * from {{ ref('int_pipeline_slippage') }}
),

latest_slippage as (
    select *
    from slippage
    qualify row_number() over (partition by company_id, opportunity_id order by snapshot_date desc) = 1
),

weighted as (
    select
        company_id,
        snapshot_date,
        opportunity_id,
        customer_id,
        scenario,
        expected_value
    from {{ ref('int_pipeline_weighted_value') }}
    where opportunity_type = 'new_biz'
),

joined as (
    select
        w.company_id,
        w.snapshot_date,
        w.opportunity_id,
        w.customer_id,
        w.scenario,
        w.expected_value,
        s.expected_start_month as month
    from weighted w
    inner join latest_slippage s
        on w.company_id = s.company_id and w.opportunity_id = s.opportunity_id and w.snapshot_date = s.snapshot_date
)
select
    company_id,
    month,
    opportunity_id,
    customer_id,
    scenario,
    'new_biz' as component,
    expected_value / 12.0 as forecast_mrr
from joined
