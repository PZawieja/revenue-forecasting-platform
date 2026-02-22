-- Bookings: contracted value that *starts* in each month (when the contract begins).
-- Grain: company_id x month x customer_id x segment.
-- Assumption: Bookings are attributed to the month of contract_start_month only (no proration).
-- bookings_mrr_equivalent = monthly MRR equivalent of new contracts starting in the month;
-- bookings_arr = 12 * bookings_mrr_equivalent (ARR attributed to that month's bookings).

with line_items as (
    select
        company_id,
        customer_id,
        contract_start_month,
        quantity,
        unit_price,
        discount_pct,
        billing_frequency,
        case
            when lower(billing_frequency) = 'annual'
            then (quantity * unit_price * (1 - coalesce(discount_pct, 0))) / 12.0
            else quantity * unit_price * (1 - coalesce(discount_pct, 0))
        end as mrr_equivalent
    from {{ ref('stg_subscription_line_items') }}
    where contract_start_month is not null
),

bookings_by_customer_month as (
    select
        company_id,
        contract_start_month as month,
        customer_id,
        sum(mrr_equivalent) as bookings_mrr_equivalent
    from line_items
    group by company_id, contract_start_month, customer_id
),

customers as (
    select company_id, customer_id, segment from {{ ref('dim_customer') }}
)

select
    b.company_id,
    b.month,
    b.customer_id,
    c.segment,
    b.bookings_mrr_equivalent,
    12.0 * b.bookings_mrr_equivalent as bookings_arr
from bookings_by_customer_month b
inner join customers c on c.company_id = b.company_id and c.customer_id = b.customer_id
