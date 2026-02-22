{% test p_renew_between_0_and_1(model, column_name) %}
-- Fails if any row has probability outside [0, 1] (common forecasting mistake: unclamped probs).
select *
from {{ model }}
where {{ column_name }} < 0 or {{ column_name }} > 1
{% endtest %}
