-- Passthrough from DuckDB ml_calibration_bins (written by forecasting calibration_reports).
-- If table does not exist, returns zero rows with the same schema.

{% if table_exists('main', 'ml_calibration_bins') == 'true' %}
select
    dataset,
    model_name,
    cutoff_month,
    bin_id,
    p_pred_mean,
    y_true_rate,
    count
from main.ml_calibration_bins
{% else %}
select
    cast(null as varchar) as dataset,
    cast(null as varchar) as model_name,
    cast(null as date) as cutoff_month,
    cast(null as integer) as bin_id,
    cast(null as double) as p_pred_mean,
    cast(null as double) as y_true_rate,
    cast(null as integer) as count
where 1 = 0
{% endif %}
