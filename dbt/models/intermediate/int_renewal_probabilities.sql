with fct as (
    select * from {{ ref('fct_subscription_line_item_monthly') }}
),

current_data_month as (
    select company_id, max(month) as month from fct group by company_id
),

contract_renewal_months as (
    select
        company_id,
        customer_id,
        contract_id,
        max(month) as renewal_month
    from fct
    group by company_id, customer_id, contract_id
),

next_renewal_per_customer as (
    select
        crm.company_id,
        crm.customer_id,
        min(crm.renewal_month) as renewal_month
    from contract_renewal_months crm
    inner join current_data_month c on c.company_id = crm.company_id and crm.renewal_month >= c.month
    group by crm.company_id, crm.customer_id
),

mrr_pre_renewal as (
    select
        nr.company_id,
        nr.customer_id,
        nr.renewal_month,
        sum(f.mrr) as current_mrr_pre_renewal
    from next_renewal_per_customer nr
    inner join fct f
        on f.company_id = nr.company_id and f.customer_id = nr.customer_id
        and f.month = (nr.renewal_month - (interval '1 month'))::date
    group by nr.company_id, nr.customer_id, nr.renewal_month
),

health_latest_before_renewal as (
    select
        nr.company_id,
        nr.customer_id,
        nr.renewal_month,
        h.health_score_1_10,
        h.trailing_3m_slope_bucket as slope_bucket
    from next_renewal_per_customer nr
    inner join {{ ref('int_customer_health_monthly') }} h
        on h.company_id = nr.company_id and h.customer_id = nr.customer_id
        and h.month < nr.renewal_month
    qualify row_number() over (
        partition by nr.company_id, nr.customer_id, nr.renewal_month
        order by h.month desc
    ) = 1
),

base_inputs as (
    select
        m.company_id,
        m.customer_id,
        m.renewal_month,
        m.current_mrr_pre_renewal,
        coalesce(h.health_score_1_10, 5) as health_score_1_10,
        coalesce(h.slope_bucket, 'flat') as slope_bucket,
        d.segment,
        d.segment_group
    from mrr_pre_renewal m
    left join health_latest_before_renewal h
        on h.company_id = m.company_id and h.customer_id = m.customer_id and h.renewal_month = m.renewal_month
    inner join {{ ref('dim_customer') }} d on d.company_id = m.company_id and d.customer_id = m.customer_id
),

segment_baseline as (
    select
        b.company_id,
        b.customer_id,
        b.renewal_month,
        b.current_mrr_pre_renewal,
        b.health_score_1_10,
        b.slope_bucket,
        b.segment,
        b.segment_group,
        sc.renewal_baseline_p as base_p,
        sc.renewal_upside_add,
        sc.renewal_downside_sub
    from base_inputs b
    inner join {{ ref('segment_config') }} sc on sc.company_id = b.company_id and sc.segment = b.segment
),

-- Deterministic rules-based renewal probability (clamped 0.05..0.99)
p_renew_rules as (
    select
        company_id,
        customer_id,
        renewal_month,
        segment,
        segment_group,
        health_score_1_10,
        slope_bucket,
        current_mrr_pre_renewal,
        renewal_upside_add,
        renewal_downside_sub,
        least(0.99, greatest(0.05,
            base_p
            + case
                when health_score_1_10 >= 8 then 0.05
                when health_score_1_10 between 6 and 7 then 0.02
                when health_score_1_10 between 4 and 5 then -0.05
                when health_score_1_10 <= 3 then -0.12
                else 0
            end
            + case slope_bucket
                when 'growing' then 0.03
                when 'flat' then 0.00
                when 'declining' then -0.04
                else 0
            end
        )) as p_renew_rules
    from segment_baseline
),

-- ML predictions: preferred model per dataset (stg_ml_renewal_predictions has p_renew_ml, p_renew_source)
with_ml_base as (
    select
        r.company_id,
        r.customer_id,
        r.renewal_month,
        r.segment,
        r.health_score_1_10,
        r.slope_bucket,
        r.current_mrr_pre_renewal,
        r.renewal_upside_add,
        r.renewal_downside_sub,
        coalesce(ml.p_renew_ml, r.p_renew_rules) as p_renew_base,
        coalesce(ml.p_renew_source, 'rules') as p_renew_source
    from p_renew_rules r
    left join {{ ref('stg_ml_renewal_predictions') }} ml
        on ml.company_id = r.company_id
        and ml.customer_id = r.customer_id
        and ml.renewal_month = r.renewal_month
),

-- Apply scenario adjustments from scenario_config on top of base (clamp 0.05..0.99)
scenarios as (
    select
        company_id,
        customer_id,
        renewal_month as month,
        segment,
        'base' as scenario,
        least(0.99, greatest(0.05, p_renew_base)) as p_renew,
        p_renew_source,
        current_mrr_pre_renewal,
        health_score_1_10,
        slope_bucket
    from with_ml_base
    union all
    select
        company_id,
        customer_id,
        renewal_month,
        segment,
        'upside',
        least(0.99, greatest(0.05, p_renew_base + renewal_upside_add)),
        p_renew_source,
        current_mrr_pre_renewal,
        health_score_1_10,
        slope_bucket
    from with_ml_base
    union all
    select
        company_id,
        customer_id,
        renewal_month,
        segment,
        'downside',
        greatest(0.05, least(0.99, p_renew_base - renewal_downside_sub)),
        p_renew_source,
        current_mrr_pre_renewal,
        health_score_1_10,
        slope_bucket
    from with_ml_base
)

select * from scenarios
