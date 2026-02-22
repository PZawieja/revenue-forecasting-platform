-- Staging for ML pipeline close predictions. Reads from DuckDB ml_pipeline_predictions when it exists;
-- filters to the preferred model for pipeline (stg_ml_model_selection), keeps latest as_of_month per model.
-- Exposes p_close_ml and p_close_source = 'ml_' || preferred_model.

{% if table_exists('main', 'ml_pipeline_predictions') == 'true' %}
with selection as (
    select preferred_model
    from {{ ref('stg_ml_model_selection') }}
    where dataset = 'pipeline'
),
raw as (
    select
        company_id,
        opportunity_id,
        snapshot_month,
        as_of_month,
        model_name,
        p_close_ml,
        created_at_utc
    from main.ml_pipeline_predictions
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
        opportunity_id,
        snapshot_month,
        p_close_ml,
        preferred_model,
        row_number() over (
            partition by company_id, opportunity_id, snapshot_month
            order by as_of_month desc nulls last
        ) as rn
    from filtered
)
select
    company_id,
    opportunity_id,
    snapshot_month,
    p_close_ml,
    'ml_' || preferred_model as p_close_source
from latest
where rn = 1
{% else %}
select
    cast(null as varchar) as company_id,
    cast(null as varchar) as opportunity_id,
    cast(null as date) as snapshot_month,
    cast(null as double) as p_close_ml,
    cast(null as varchar) as p_close_source
where 1 = 0
{% endif %}
