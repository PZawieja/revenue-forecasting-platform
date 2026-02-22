with source as (
    select * from {{ ref('subscription_line_items') }}
),

renamed as (
    select
        cast(company_id as varchar) as company_id,
        contract_id,
        customer_id,
        product_id,
        cast(contract_start_date as date) as contract_start_date,
        cast(contract_end_date as date) as contract_end_date,
        lower(billing_frequency) as billing_frequency,
        quantity::integer as quantity,
        cast(unit_price as double) as unit_price,
        cast(coalesce(discount_pct, 0) as double) as discount_pct,
        lower(status) as status,
        date_trunc('month', cast(contract_start_date as date))::date as contract_start_month,
        date_trunc('month', cast(contract_end_date as date))::date as contract_end_month,
        date_trunc('month', cast(contract_end_date as date))::date as renewal_month
    from source
)

select * from renamed
