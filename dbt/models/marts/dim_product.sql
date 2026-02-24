-- Normalize product_family to contract values (a, b, c, d); map unknown to 'd'.
select
    company_id,
    product_id,
    case
        when lower(trim(product_family)) in ('a', 'b', 'c', 'd') then lower(trim(product_family))
        else 'd'
    end as product_family,
    is_recurring,
    default_term_months
from {{ ref('stg_products') }}
