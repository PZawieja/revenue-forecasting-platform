-- Cohort-based churn and revenue retention by tenure. Grain: cohort_month x tenure_month x segment.
-- Definitions: tenure_month = months since cohort_month; logo = customer still has subscription in that month;
-- survival_rate_logo = share of cohort still active; survival_rate_revenue = revenue in tenure month / revenue in month 0.
with cohorts as (
    select company_id, customer_id, cohort_month
    from {{ ref('int_customer_cohorts') }}
),

cohorts_with_segment as (
    select
        c.company_id,
        c.customer_id,
        c.cohort_month,
        d.segment
    from cohorts c
    inner join {{ ref('dim_customer') }} d on d.company_id = c.company_id and d.customer_id = c.customer_id
),

cohort_sizes as (
    select
        company_id,
        cohort_month,
        segment,
        count(*) as cohort_size
    from cohorts_with_segment
    group by company_id, cohort_month, segment
),

mrr_monthly as (
    select
        company_id,
        customer_id,
        month,
        sum(mrr) as mrr
    from {{ ref('fct_subscription_line_item_monthly') }}
    group by company_id, customer_id, month
),

tenure_spine as (
    select
        cws.company_id,
        cws.cohort_month,
        t.tenure_month,
        cws.segment
    from (select distinct company_id, cohort_month, segment from cohorts_with_segment) cws
    cross join (select unnest(generate_series(0, 59)) as tenure_month) t
),

with_calendar_month as (
    select
        company_id,
        cohort_month,
        tenure_month,
        segment,
        (cohort_month + (tenure_month * interval '1 month'))::date as calendar_month
    from tenure_spine
),

-- Active customers (logo) and revenue at each (cohort_month, tenure_month, segment).
-- active_customers = count of cohort members with mrr in that calendar month; cohort_revenue = sum of that mrr.
activity as (
    select
        w.company_id,
        w.cohort_month,
        w.tenure_month,
        w.segment,
        count(distinct case when m.customer_id is not null then cws.customer_id end) as active_customers,
        coalesce(sum(m.mrr), 0) as cohort_revenue
    from with_calendar_month w
    inner join cohorts_with_segment cws
        on cws.company_id = w.company_id and cws.cohort_month = w.cohort_month and cws.segment = w.segment
    left join mrr_monthly m
        on m.company_id = cws.company_id and m.customer_id = cws.customer_id and m.month = w.calendar_month
    group by w.company_id, w.cohort_month, w.tenure_month, w.segment
),

revenue_at_tenure_0 as (
    select
        cws.company_id,
        cws.cohort_month,
        cws.segment,
        coalesce(sum(m.mrr), 0) as revenue_0
    from cohorts_with_segment cws
    left join mrr_monthly m
        on m.company_id = cws.company_id and m.customer_id = cws.customer_id and m.month = cws.cohort_month
    group by cws.company_id, cws.cohort_month, cws.segment
),

with_rates as (
    select
        a.company_id,
        a.cohort_month,
        a.tenure_month,
        a.segment,
        cs.cohort_size,
        a.active_customers,
        a.cohort_revenue,
        r.revenue_0,
        case when cs.cohort_size > 0 then a.active_customers::double / cs.cohort_size else null end as survival_rate_logo,
        case when r.revenue_0 > 0 then a.cohort_revenue / r.revenue_0 else null end as survival_rate_revenue
    from activity a
    inner join cohort_sizes cs
        on cs.company_id = a.company_id and cs.cohort_month = a.cohort_month and cs.segment = a.segment
    inner join revenue_at_tenure_0 r
        on r.company_id = a.company_id and r.cohort_month = a.cohort_month and r.segment = a.segment
)

select
    company_id,
    cohort_month,
    tenure_month,
    segment,
    survival_rate_logo,
    survival_rate_revenue
from with_rates
