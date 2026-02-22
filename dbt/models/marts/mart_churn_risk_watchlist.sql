-- Churn risk watchlist: customers with renewal in next 3 months OR health_score <= 4 OR declining usage.
-- Grain: company_id x month x customer_id. One row per at-risk customer per report month.
-- risk_reason concatenates applicable reasons; rank within segment by risk severity (declining + low health first, then health asc, months_to_renewal asc).

with spine as (
    select month as report_month from {{ ref('int_month_spine') }}
),

company_customers as (
    select distinct company_id, customer_id, segment from {{ ref('dim_customer') }}
),

-- Customers with a renewal in the next 3 months (from report month)
renewal_next_3m as (
    select distinct
        r.company_id,
        s.report_month as month,
        r.customer_id,
        r.segment,
        r.month as renewal_month,
        date_diff('month', s.report_month, r.month) as months_to_renewal,
        r.p_renew,
        r.current_mrr_pre_renewal,
        r.health_score_1_10,
        r.slope_bucket
    from {{ ref('int_renewal_probabilities') }} r
    cross join spine s
    where r.scenario = 'base'
      and r.month > s.report_month
      and r.month <= (s.report_month + interval '3 months')::date
),

-- Health and slope by month (for health <= 4 or declining)
health_monthly as (
    select
        company_id,
        month,
        customer_id,
        health_score_1_10,
        trailing_3m_slope_bucket as slope_bucket
    from {{ ref('int_customer_health_monthly') }}
),

at_risk_health as (
    select distinct
        h.company_id,
        h.month,
        h.customer_id,
        c.segment,
        h.health_score_1_10,
        h.slope_bucket
    from health_monthly h
    inner join company_customers c on c.company_id = h.company_id and c.customer_id = h.customer_id
    where h.health_score_1_10 <= 4 or h.trailing_3m_slope_bucket = 'declining'
),

-- Current ARR by customer and month (12 * MRR)
customer_arr as (
    select
        company_id,
        customer_id,
        month,
        12.0 * sum(mrr) as current_arr
    from {{ ref('fct_subscription_line_item_monthly') }}
    group by company_id, customer_id, month
),

-- Union: (company_id, month, customer_id) from renewal in 3m OR health/slope at-risk
watchlist_keys as (
    select company_id, month, customer_id from renewal_next_3m
    union
    select company_id, month, customer_id from at_risk_health
),

-- Enrich: segment, months_to_renewal, current_arr, p_renew, health, slope, risk_reason
enriched as (
    select
        w.company_id,
        w.month,
        w.customer_id,
        coalesce(r.segment, a.segment) as segment,
        r.months_to_renewal,
        coalesce(ca.current_arr, 12.0 * r.current_mrr_pre_renewal, 0) as current_arr,
        r.p_renew,
        coalesce(r.health_score_1_10, a.health_score_1_10) as health_score_1_10,
        coalesce(r.slope_bucket, a.slope_bucket) as slope_bucket,
        trim(
            coalesce(case when r.customer_id is not null then 'Renewal in ' || r.months_to_renewal::varchar || ' months; ' else '' end, '') ||
            coalesce(case when coalesce(r.health_score_1_10, a.health_score_1_10) <= 4 then 'Low health (' || coalesce(r.health_score_1_10, a.health_score_1_10)::varchar || '); ' else '' end, '') ||
            coalesce(case when coalesce(r.slope_bucket, a.slope_bucket) = 'declining' then 'Declining usage' else '' end, '')
        ) as risk_reason
    from watchlist_keys w
    left join renewal_next_3m r on r.company_id = w.company_id and r.month = w.month and r.customer_id = w.customer_id
    left join at_risk_health a on a.company_id = w.company_id and a.month = w.month and a.customer_id = w.customer_id
    left join customer_arr ca on ca.company_id = w.company_id and ca.customer_id = w.customer_id and ca.month = w.month
),

ranked as (
    select
        company_id,
        month,
        customer_id,
        segment,
        months_to_renewal,
        current_arr,
        p_renew,
        health_score_1_10,
        slope_bucket,
        risk_reason,
        row_number() over (
            partition by company_id, month, segment
            order by
                case when slope_bucket = 'declining' then 0 else 1 end,
                case when health_score_1_10 <= 4 then 0 else 1 end,
                health_score_1_10 asc nulls last,
                months_to_renewal asc nulls last,
                customer_id
        ) as risk_rank
    from enriched
)

select
    company_id,
    month,
    customer_id,
    segment,
    months_to_renewal,
    current_arr,
    p_renew,
    health_score_1_10,
    slope_bucket,
    risk_reason,
    risk_rank
from ranked
order by company_id, month, segment, risk_rank
