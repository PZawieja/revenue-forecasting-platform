with source as (
    select * from {{ ref('customers') }}
),

renamed as (
    select
        cast(company_id as varchar) as company_id,
        customer_id,
        customer_name,
        lower(segment) as segment,
        region,
        industry,
        crm_health_input::integer as crm_health_input,
        cast(created_date as date) as created_date,
        case
            when lower(segment) in ('enterprise', 'large') then 'enterprise_large'
            else 'mid_smb'
        end as segment_group
    from source
)

select * from renamed
