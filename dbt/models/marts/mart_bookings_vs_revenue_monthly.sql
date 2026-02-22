-- Bookings vs revenue recognition by month and segment.
-- Grain: company_id x month x segment.
-- Assumption: Revenue recognition is simplified as monthly MRR (recognized_arr = 12 * actual_mrr);
-- no deferred revenue schedule or GAAP allocation—recognized = actual MRR in that month expressed as ARR.
-- bookings_arr = ARR from contracts that *started* in the month (from int_bookings_monthly).
-- bookings_to_revenue_ratio = bookings_arr / nullif(recognized_arr, 0) (ratio of booked to recognized in same month).
-- Rolling 3m = sum of current and two preceding months.

with spine as (
    select month from {{ ref('int_month_spine') }}
),

company_segments as (
    select distinct company_id, segment from {{ ref('dim_customer') }}
),

month_segment as (
    select s.month, cs.company_id, cs.segment
    from spine s
    cross join company_segments cs
),

bookings_agg as (
    select
        company_id,
        month,
        segment,
        sum(bookings_arr) as bookings_arr
    from {{ ref('int_bookings_monthly') }}
    group by company_id, month, segment
),

actuals as (
    select
        company_id,
        month,
        segment,
        12.0 * sum(mrr) as recognized_arr
    from {{ ref('fct_revenue_actuals_monthly') }}
    group by company_id, month, segment
),

joined as (
    select
        m.company_id,
        m.month,
        m.segment,
        coalesce(b.bookings_arr, 0) as bookings_arr,
        coalesce(a.recognized_arr, 0) as recognized_arr
    from month_segment m
    left join bookings_agg b on b.company_id = m.company_id and b.month = m.month and b.segment = m.segment
    left join actuals a on a.company_id = m.company_id and a.month = m.month and a.segment = m.segment
),

with_rolling as (
    select
        company_id,
        month,
        segment,
        bookings_arr,
        recognized_arr,
        bookings_arr / nullif(recognized_arr, 0) as bookings_to_revenue_ratio,
        sum(bookings_arr) over (
            partition by company_id, segment
            order by month
            rows between 2 preceding and current row
        ) as rolling_3m_bookings_arr,
        sum(recognized_arr) over (
            partition by company_id, segment
            order by month
            rows between 2 preceding and current row
        ) as rolling_3m_recognized_arr
    from joined
)

select
    company_id,
    month,
    segment,
    bookings_arr,
    recognized_arr,
    bookings_to_revenue_ratio,
    rolling_3m_bookings_arr,
    rolling_3m_recognized_arr
from with_rolling
