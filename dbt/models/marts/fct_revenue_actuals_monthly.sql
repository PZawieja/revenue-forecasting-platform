with line_item_monthly as (
    select * from {{ ref('fct_subscription_line_item_monthly') }}
),

customers as (
    select company_id, customer_id, segment from {{ ref('dim_customer') }}
),

revenue as (
    select
        lim.company_id,
        lim.month,
        lim.customer_id,
        c.segment,
        sum(lim.mrr) as mrr
    from line_item_monthly lim
    inner join customers c on lim.company_id = c.company_id and lim.customer_id = c.customer_id
    group by
        lim.company_id,
        lim.month,
        lim.customer_id,
        c.segment
)

select
    company_id,
    month,
    segment,
    sum(mrr) as mrr
from revenue
group by company_id, month, segment
