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
        expected_close_month
    from {{ ref('stg_pipeline_opportunities_snapshot') }}
),

probs as (
    select * from {{ ref('int_stage_probabilities') }}
),

joined as (
    select
        p.company_id,
        p.snapshot_date,
        p.snapshot_month,
        p.opportunity_id,
        p.customer_id,
        p.segment,
        p.opportunity_type,
        p.stage,
        p.amount,
        p.expected_close_month,
        pr.p_base,
        pr.p_upside,
        pr.p_downside
    from pipe p
    left join probs pr
        on p.company_id = pr.company_id and p.segment = pr.segment and p.stage = pr.stage
),

scenarios as (
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
        'base' as scenario,
        coalesce(p_base, 0.5) as p_close,
        amount * coalesce(p_base, 0.5) as expected_value
    from joined
    union all
    select
        company_id, snapshot_date, snapshot_month, opportunity_id, customer_id, segment, opportunity_type, stage, amount, expected_close_month,
        'upside', coalesce(p_upside, 0.5), amount * coalesce(p_upside, 0.5)
    from joined
    union all
    select
        company_id, snapshot_date, snapshot_month, opportunity_id, customer_id, segment, opportunity_type, stage, amount, expected_close_month,
        'downside', coalesce(p_downside, 0.5), amount * coalesce(p_downside, 0.5)
    from joined
)

select * from scenarios
