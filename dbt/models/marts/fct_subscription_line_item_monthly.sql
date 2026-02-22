with line_items as (
    select * from {{ ref('stg_subscription_line_items') }}
),

months as (
    select unnest(generate_series(0, 59)) as month_offset
),

expanded as (
    select
        li.company_id,
        li.customer_id,
        li.product_id,
        li.contract_id,
        (li.contract_start_month + (interval '1 month' * m.month_offset))::date as month,
        li.quantity,
        li.unit_price,
        li.discount_pct,
        li.billing_frequency,
        li.status,
        case
            when li.billing_frequency = 'annual'
            then (li.quantity * li.unit_price * (1 - li.discount_pct)) / 12.0
            else li.quantity * li.unit_price * (1 - li.discount_pct)
        end as mrr
    from line_items li
    cross join months m
    where (li.contract_start_month + (interval '1 month' * m.month_offset))::date <= li.contract_end_month
)

select * from expanded
