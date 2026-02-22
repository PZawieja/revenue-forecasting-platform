-- Fails if any (company_id, customer_id) in stg_subscription_line_items does not exist in stg_customers.
select li.company_id, li.customer_id
from {{ ref('stg_subscription_line_items') }} li
left join {{ ref('stg_customers') }} c
    on c.company_id = li.company_id and c.customer_id = li.customer_id
where c.customer_id is null
