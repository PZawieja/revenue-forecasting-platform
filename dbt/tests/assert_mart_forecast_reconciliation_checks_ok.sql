-- Fails if any row has forecast_reconciliation_ok_flag = false (reconciliation audit).

select company_id, month, segment, scenario,
  forecast_mrr_total, component_sum_mrr, forecast_component_diff, forecast_reconciliation_ok_flag
from {{ ref('mart_forecast_reconciliation_checks') }}
where forecast_reconciliation_ok_flag = false
