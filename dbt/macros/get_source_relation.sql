{% macro get_source_relation(table_name) %}
{% if var('data_mode') == 'sim' %}
select * from read_parquet('{{ var("sim_data_path") }}/{{ table_name }}.parquet')
{% else %}
select * from {{ ref(table_name) }}
{% endif %}
{% endmacro %}
