-- Slippage months from slippage_config (company_id, segment_group, stage).
with pipe as (
    select
        company_id,
        snapshot_date,
        snapshot_month,
        opportunity_id,
        customer_id,
        segment,
        opportunity_type,
        stage,
        amount,
        expected_close_month,
        case
            when segment in ('enterprise', 'large') then 'enterprise_large'
            else 'mid_smb'
        end as segment_group
    from {{ ref('stg_pipeline_opportunities_snapshot') }}
),

with_slippage as (
    select
        p.company_id,
        p.snapshot_date,
        p.snapshot_month,
        p.opportunity_id,
        p.customer_id,
        p.segment,
        p.segment_group,
        p.opportunity_type,
        p.stage,
        p.amount,
        p.expected_close_month,
        coalesce(s.slippage_months, 0)::integer as slippage_months,
        (p.expected_close_month + (interval '1 month' * coalesce(s.slippage_months, 0)))::date as expected_start_month
    from pipe p
    left join {{ ref('slippage_config') }} s
        on s.company_id = p.company_id and s.segment_group = p.segment_group and s.stage = p.stage
)

select * from with_slippage
