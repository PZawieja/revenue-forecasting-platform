-- When data_mode=sim, this model errors clearly if sim Parquet files are missing.
-- Run ./scripts/sim_generate.sh before dbt run --vars '{data_mode: sim}'.
{% if var('data_mode') == 'sim' %}
select 1 as sim_data_ok
from read_parquet('{{ var("sim_data_path") }}/customers.parquet')
limit 1
{% else %}
select 1 as sim_data_ok
{% endif %}
