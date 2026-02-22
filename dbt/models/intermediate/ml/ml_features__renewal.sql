-- Features for renewal ML model. One row per (company_id, customer_id, renewal_month).
-- All features are as-of as_of_month = renewal_month - 1 month (no post-renewal leakage).
-- Multi-company; time-valid for training and scoring.

with renewal_months as (
    select distinct
        company_id,
        customer_id,
        contract_end_month as renewal_month
    from {{ ref('stg_subscription_line_items') }}
    where contract_end_month is not null
),

as_of_month as (
    select
        company_id,
        customer_id,
        renewal_month,
        (renewal_month - interval '1 month')::date as as_of_month
    from renewal_months
),

mrr_pre_renewal as (
    select
        a.company_id,
        a.customer_id,
        a.renewal_month,
        a.as_of_month,
        coalesce(sum(f.mrr), 0) as current_mrr_pre_renewal
    from as_of_month a
    left join {{ ref('fct_subscription_line_item_monthly') }} f
        on f.company_id = a.company_id
        and f.customer_id = a.customer_id
        and f.month = a.as_of_month
    group by a.company_id, a.customer_id, a.renewal_month, a.as_of_month
),

health_as_of as (
    select
        h.company_id,
        h.customer_id,
        h.month as as_of_month,
        h.segment,
        h.segment_group,
        h.health_score_1_10,
        h.usage_per_user_total,
        h.trailing_3m_avg_usage_per_user_total,
        h.trailing_3m_slope_bucket
    from {{ ref('int_customer_health_monthly') }} h
),

features as (
    select
        m.company_id,
        m.customer_id,
        m.renewal_month,
        m.as_of_month,
        m.current_mrr_pre_renewal,
        h.segment,
        h.segment_group,
        h.health_score_1_10,
        h.usage_per_user_total,
        h.trailing_3m_avg_usage_per_user_total,
        h.trailing_3m_slope_bucket,
        1 as months_to_renewal
    from mrr_pre_renewal m
    left join health_as_of h
        on h.company_id = m.company_id
        and h.customer_id = m.customer_id
        and h.as_of_month = m.as_of_month
)

select * from features
