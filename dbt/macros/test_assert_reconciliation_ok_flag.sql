{% test assert_reconciliation_ok_flag(model, column_name) %}
-- Fails if any row has the reconciliation ok flag = false (audit).
select *
from {{ model }}
where {{ column_name }} = false
{% endtest %}
