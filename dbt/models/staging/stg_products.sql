with src as (
    {{ get_source_relation('products') }}
),

renamed as (
    select
        cast(company_id as varchar) as company_id,
        product_id,
        lower(product_family) as product_family,
        cast(is_recurring as boolean) as is_recurring,
        default_term_months::integer as default_term_months
    from src
)

select * from renamed
