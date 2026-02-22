-- ML feature table for renewal model. Grain: company_id x customer_id x renewal_month.
-- Stable training data: features as-of month prior to renewal; label from actuals (renewed in month after).
-- Materialized as table for fast Python reads.

{{ config(materialized='table') }}

with subs as (
    select
        company_id,
        customer_id,
        contract_end_month as renewal_month
    from {{ ref('stg_subscription_line_items') }}
    where contract_end_month is not null
),

renewal_spine as (
    select distinct company_id, customer_id, renewal_month from subs
),

as_of_month as (
    select
        company_id,
        customer_id,
        renewal_month,
        (renewal_month - interval '1 month')::date as as_of_month
    from renewal_spine
),

-- Latest month in data (for months_to_renewal)
latest_month as (
    select max(month) as max_month from {{ ref('fct_subscription_line_item_monthly') }}
),

-- MRR in month prior to renewal
mrr_pre as (
    select
        a.company_id,
        a.customer_id,
        a.renewal_month,
        coalesce(sum(f.mrr), 0) as current_mrr_pre_renewal
    from as_of_month a
    left join {{ ref('fct_subscription_line_item_monthly') }} f
        on f.company_id = a.company_id
        and f.customer_id = a.customer_id
        and f.month = a.as_of_month
    group by a.company_id, a.customer_id, a.renewal_month
),

-- Health and usage as-of month prior to renewal
health_as_of as (
    select
        company_id,
        customer_id,
        month as as_of_month,
        segment,
        segment_group,
        health_score_1_10,
        trailing_3m_slope_bucket,
        trailing_3m_avg_usage_per_user_total as trailing_3m_usage_per_user
    from {{ ref('int_customer_health_monthly') }}
),

-- Cohort for tenure
cohorts as (
    select company_id, customer_id, cohort_month from {{ ref('int_customer_cohorts') }}
),

-- Label: renewed = 1 if customer has >0 MRR in month after renewal_month
mrr_after as (
    select
        company_id,
        customer_id,
        month,
        sum(mrr) as mrr_after_renewal
    from {{ ref('fct_subscription_line_item_monthly') }}
    group by company_id, customer_id, month
),

labels as (
    select
        r.company_id,
        r.customer_id,
        r.renewal_month,
        case when coalesce(m.mrr_after_renewal, 0) > 0 then 1 else 0 end as renewed_flag
    from renewal_spine r
    left join mrr_after m
        on m.company_id = r.company_id
        and m.customer_id = r.customer_id
        and m.month = (r.renewal_month + interval '1 month')::date
),

features as (
    select
        m.company_id,
        m.customer_id,
        c.segment,
        c.segment_group,
        m.renewal_month,
        m.current_mrr_pre_renewal,
        date_diff('month', l.max_month, m.renewal_month) as months_to_renewal,
        h.health_score_1_10,
        h.trailing_3m_slope_bucket as slope_bucket,
        h.trailing_3m_usage_per_user,
        date_diff('month', co.cohort_month, m.renewal_month) as tenure_months,
        lb.renewed_flag
    from mrr_pre m
    cross join latest_month l
    left join {{ ref('dim_customer') }} c
        on c.company_id = m.company_id and c.customer_id = m.customer_id
    left join health_as_of h
        on h.company_id = m.company_id and h.customer_id = m.customer_id
        and h.as_of_month = (m.renewal_month - interval '1 month')::date
    left join cohorts co
        on co.company_id = m.company_id and co.customer_id = m.customer_id
    left join labels lb
        on lb.company_id = m.company_id and lb.customer_id = m.customer_id and lb.renewal_month = m.renewal_month
)

select * from features
