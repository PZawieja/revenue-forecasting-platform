-- Fails if any (company_id, product_id) in stg_subscription_line_items does not exist in stg_products.
select li.company_id, li.product_id
from {{ ref('stg_subscription_line_items') }} li
left join {{ ref('stg_products') }} p
    on p.company_id = li.company_id and p.product_id = li.product_id
where p.product_id is null
