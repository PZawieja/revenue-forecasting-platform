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

-- Rules-based close probabilities from stage (p_base, p_upside, p_downside)
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
        pr.p_base as p_close_rules_base,
        pr.p_upside as p_close_rules_upside,
        pr.p_downside as p_close_rules_downside
    from pipe p
    left join probs pr
        on p.company_id = pr.company_id and p.segment = pr.segment and p.stage = pr.stage
),

-- ML predictions: preferred model per dataset (stg_ml_pipeline_predictions has p_close_ml, p_close_source)
with_ml_base as (
    select
        j.company_id,
        j.snapshot_date,
        j.snapshot_month,
        j.opportunity_id,
        j.customer_id,
        j.segment,
        j.opportunity_type,
        j.stage,
        j.amount,
        j.expected_close_month,
        coalesce(ml.p_close_ml, j.p_close_rules_base) as p_close_base,
        coalesce(ml.p_close_source, 'rules') as p_close_source
    from joined j
    left join {{ ref('stg_ml_pipeline_predictions') }} ml
        on ml.company_id = j.company_id
        and ml.opportunity_id = j.opportunity_id
        and ml.snapshot_month = j.snapshot_month
),

scenario_config as (
    select scenario, stage_probability_adjustment
    from {{ ref('scenario_config') }}
),

-- Apply scenario adjustment from scenario_config on top of p_close_base (clamp 0..1)
scenarios as (
    select
        b.company_id,
        b.snapshot_date,
        b.snapshot_month,
        b.opportunity_id,
        b.customer_id,
        b.segment,
        b.opportunity_type,
        b.stage,
        b.amount,
        b.expected_close_month,
        s.scenario,
        least(1.0, greatest(0.0, b.p_close_base + coalesce(s.stage_probability_adjustment, 0))) as p_close,
        b.amount * least(1.0, greatest(0.0, b.p_close_base + coalesce(s.stage_probability_adjustment, 0))) as expected_value,
        b.p_close_source
    from with_ml_base b
    cross join scenario_config s
)

select * from scenarios
