with source as (
    select * from {{ ref('usage_monthly') }}
),

renamed as (
    select
        cast(company_id as varchar) as company_id,
        cast(month as date) as month,
        customer_id,
        lower(feature_key) as feature_key,
        usage_count::double as usage_count,
        active_users::integer as active_users,
        usage_count::double / nullif(active_users::double, 0) as usage_per_user
    from source
)

select * from renamed
