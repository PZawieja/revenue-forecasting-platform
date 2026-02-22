with renewal as (
    select
        company_id,
        month,
        customer_id,
        scenario,
        current_mrr_pre_renewal,
        p_renew
    from {{ ref('int_renewal_probabilities') }}
)
select
    company_id,
    month,
    customer_id,
    scenario,
    'renewal' as component,
    current_mrr_pre_renewal * p_renew as forecast_mrr
from renewal
