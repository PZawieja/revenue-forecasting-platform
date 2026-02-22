-- Passthrough from DuckDB ml_threshold_metrics (written by forecasting calibration_reports).
-- If table does not exist, returns zero rows with the same schema.

{% if table_exists('main', 'ml_threshold_metrics') == 'true' %}
select
    dataset,
    model_name,
    cutoff_month,
    threshold,
    predicted_positive,
    tp,
    fp,
    tn,
    fn,
    precision,
    recall,
    fpr,
    fnr
from main.ml_threshold_metrics
{% else %}
select
    cast(null as varchar) as dataset,
    cast(null as varchar) as model_name,
    cast(null as date) as cutoff_month,
    cast(null as double) as threshold,
    cast(null as integer) as predicted_positive,
    cast(null as integer) as tp,
    cast(null as integer) as fp,
    cast(null as integer) as tn,
    cast(null as integer) as fn,
    cast(null as double) as precision,
    cast(null as double) as recall,
    cast(null as double) as fpr,
    cast(null as double) as fnr
where 1 = 0
{% endif %}
