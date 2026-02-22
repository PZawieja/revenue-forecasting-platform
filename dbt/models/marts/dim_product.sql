select
    company_id,
    product_id,
    product_family,
    is_recurring,
    default_term_months
from {{ ref('stg_products') }}
