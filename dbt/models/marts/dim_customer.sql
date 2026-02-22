select
    company_id,
    customer_id,
    customer_name,
    segment,
    segment_group,
    region,
    industry,
    created_date
from {{ ref('stg_customers') }}
