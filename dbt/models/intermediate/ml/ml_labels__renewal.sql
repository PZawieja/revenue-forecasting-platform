-- Labels for renewal ML model. One row per (company_id, customer_id, renewal_month).
-- renewed = 1: customer has active/non_renewing revenue in renewal_month or in the next 2 months.
-- churned = 0: customer has 0 revenue in renewal_month and in the next 2 months (avoids noisy gaps).
-- Only rows where we have data for renewal_month, renewal_month+1, renewal_month+2 are labeled.

with fct as (
    select
        company_id,
        customer_id,
        month,
        sum(mrr) as customer_mrr
    from {{ ref('fct_subscription_line_item_monthly') }}
    where status in ('active', 'non_renewing')
    group by company_id, customer_id, month
),

renewal_months as (
    select distinct
        company_id,
        customer_id,
        contract_end_month as renewal_month
    from {{ ref('stg_subscription_line_items') }}
    where contract_end_month is not null
),

revenue_in_window as (
    select
        r.company_id,
        r.customer_id,
        r.renewal_month,
        coalesce(max(case when f.month = r.renewal_month then f.customer_mrr end), 0) as mrr_0,
        coalesce(max(case when f.month = (r.renewal_month + interval '1 month')::date then f.customer_mrr end), 0) as mrr_1,
        coalesce(max(case when f.month = (r.renewal_month + interval '2 months')::date then f.customer_mrr end), 0) as mrr_2
    from renewal_months r
    left join fct f
        on f.company_id = r.company_id
        and f.customer_id = r.customer_id
        and f.month in (
            r.renewal_month,
            (r.renewal_month + interval '1 month')::date,
            (r.renewal_month + interval '2 months')::date
        )
    group by r.company_id, r.customer_id, r.renewal_month
),

labeled as (
    select
        company_id,
        customer_id,
        renewal_month,
        case
            when mrr_0 > 0 or mrr_1 > 0 or mrr_2 > 0 then 1
            else 0
        end as label_renewed
    from revenue_in_window
)

select * from labeled
