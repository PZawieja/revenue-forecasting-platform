-- Fails if any row has p_renew outside [0, 1]
select
    month,
    customer_id,
    scenario,
    p_renew
from {{ ref('int_renewal_probabilities') }}
where p_renew < 0 or p_renew > 1
