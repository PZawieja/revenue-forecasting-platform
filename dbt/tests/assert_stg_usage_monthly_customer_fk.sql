-- Fails if any (company_id, customer_id) in stg_usage_monthly does not exist in stg_customers.
select u.company_id, u.customer_id
from {{ ref('stg_usage_monthly') }} u
left join {{ ref('stg_customers') }} c
    on c.company_id = u.company_id and c.customer_id = u.customer_id
where c.customer_id is null
