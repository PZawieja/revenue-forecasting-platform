with source as (
    select * from {{ ref('pipeline_opportunities_snapshot') }}
),

renamed as (
    select
        cast(company_id as varchar) as company_id,
        cast(snapshot_date as date) as snapshot_date,
        opportunity_id,
        customer_id,
        lower(segment) as segment,
        lower(stage) as stage,
        cast(amount as double) as amount,
        cast(expected_close_date as date) as expected_close_date,
        lower(opportunity_type) as opportunity_type,
        date_trunc('month', cast(snapshot_date as date))::date as snapshot_month,
        date_trunc('month', cast(expected_close_date as date))::date as expected_close_month
    from source
)

select * from renamed
