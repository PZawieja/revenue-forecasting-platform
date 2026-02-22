-- Data contract test: numeric column must be within [min_value, max_value] (inclusive).
-- Usage in schema.yml:
--   - accepted_range:
--       min_value: 0
--       max_value: 10
-- Fails if any row has value < min_value or value > max_value (nulls ignored).
{% test accepted_range(model, column_name, min_value=none, max_value=none) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and (
    {% if min_value is not none %}{{ column_name }} < {{ min_value }}{% endif %}
    {% if min_value is not none and max_value is not none %} or {% endif %}
    {% if max_value is not none %}{{ column_name }} > {{ max_value }}{% endif %}
    {% if min_value is none and max_value is none %}1 = 0{% endif %}
  )
{% endtest %}
