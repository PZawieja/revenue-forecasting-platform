{% macro table_exists(schema_name, table_name) %}
{% set sql %}
SELECT 1 FROM information_schema.tables
WHERE table_schema = '{{ schema_name }}' AND table_name = '{{ table_name }}'
LIMIT 1
{% endset %}
{% set result = run_query(sql) %}
{% if result and result.rows and result.rows | length > 0 %}
  {{ return('true') }}
{% endif %}
{{ return('false') }}
{% endmacro %}
