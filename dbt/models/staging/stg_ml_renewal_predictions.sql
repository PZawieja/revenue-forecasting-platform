-- Staging for ML renewal predictions. Reads from DuckDB ml_renewal_predictions when it exists;
-- filters to the preferred model for renewals (stg_ml_model_selection), keeps latest as_of_month.
-- Exposes p_renew_ml and p_renew_source = 'ml_' || preferred_model.

{% if table_exists('main', 'ml_renewal_predictions') == 'true' %}
with selection as (
    select preferred_model
    from {{ ref('stg_ml_model_selection') }}
    where dataset = 'renewals'
),
raw as (
    select
        company_id,
        customer_id,
        renewal_month,
        as_of_month,
        model_name,
        p_renew_ml,
        created_at_utc
    from main.ml_renewal_predictions
),
filtered as (
    select r.*, s.preferred_model
    from raw r
    cross join selection s
    where r.model_name = s.preferred_model
),
latest as (
    select
        company_id,
        customer_id,
        renewal_month,
        p_renew_ml,
        preferred_model,
        row_number() over (
            partition by company_id, customer_id, renewal_month
            order by as_of_month desc nulls last
        ) as rn
    from filtered
)
select
    company_id,
    customer_id,
    renewal_month,
    p_renew_ml,
    'ml_' || preferred_model as p_renew_source
from latest
where rn = 1
{% else %}
select
    cast(null as varchar) as company_id,
    cast(null as varchar) as customer_id,
    cast(null as date) as renewal_month,
    cast(null as double) as p_renew_ml,
    cast(null as varchar) as p_renew_source
where 1 = 0
{% endif %}
