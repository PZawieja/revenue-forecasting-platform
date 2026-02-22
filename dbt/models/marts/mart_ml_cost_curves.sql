-- Passthrough from DuckDB ml_cost_curves (written by forecasting calibration_reports).
-- If table does not exist, returns zero rows with the same schema.

{% if table_exists('main', 'ml_cost_curves') == 'true' %}
select
    dataset,
    model_name,
    cutoff_month,
    threshold,
    expected_cost
from main.ml_cost_curves
{% else %}
select
    cast(null as varchar) as dataset,
    cast(null as varchar) as model_name,
    cast(null as date) as cutoff_month,
    cast(null as double) as threshold,
    cast(null as double) as expected_cost
where 1 = 0
{% endif %}
