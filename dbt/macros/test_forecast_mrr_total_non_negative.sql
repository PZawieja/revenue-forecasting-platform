{% test forecast_mrr_total_non_negative(model, column_name) %}
-- Fails if any row has negative total forecast (allow 0, disallow negative).
select *
from {{ model }}
where {{ column_name }} < 0
{% endtest %}
