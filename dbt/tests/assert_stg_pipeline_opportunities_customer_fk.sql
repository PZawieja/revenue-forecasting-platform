-- Fails if any non-null (company_id, customer_id) in pipeline does not exist in stg_customers.
select pipe.company_id, pipe.customer_id
from {{ ref('stg_pipeline_opportunities_snapshot') }} pipe
left join {{ ref('stg_customers') }} c
    on c.company_id = pipe.company_id and c.customer_id = pipe.customer_id
where pipe.customer_id is not null and c.customer_id is null
