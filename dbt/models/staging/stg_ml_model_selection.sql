-- Preferred ML model per dataset for forecast pipelines. Read from DuckDB ml_model_selection
-- (written by forecasting publish_model_selection); if table does not exist, default to logistic for both.
-- Ensures exactly one row per dataset.

{% if table_exists('main', 'ml_model_selection') == 'true' %}
with raw as (
    select dataset, preferred_model, updated_at_utc
    from main.ml_model_selection
),
one_per_dataset as (
    select
        dataset,
        preferred_model,
        updated_at_utc,
        row_number() over (partition by dataset order by updated_at_utc desc nulls last) as rn
    from raw
)
select dataset, preferred_model, updated_at_utc
from one_per_dataset
where rn = 1
{% else %}
select 'renewals' as dataset, 'logistic' as preferred_model, cast(null as timestamp) as updated_at_utc
union all
select 'pipeline' as dataset, 'logistic' as preferred_model, cast(null as timestamp) as updated_at_utc
{% endif %}
