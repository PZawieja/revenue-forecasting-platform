-- Customer cohorts by first contract start month. Grain: company_id x customer_id.
-- cohort_month = min(contract_start_month) across all contracts for that customer.
with source as (
    select
        company_id,
        customer_id,
        contract_start_month
    from {{ ref('stg_subscription_line_items') }}
)

select
    company_id,
    customer_id,
    min(contract_start_month)::date as cohort_month
from source
group by company_id, customer_id
