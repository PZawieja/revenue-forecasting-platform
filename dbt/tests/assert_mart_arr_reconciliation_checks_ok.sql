-- Fails if any row has arr_reconciliation_ok_flag = false (reconciliation audit).

select company_id, month, segment, scenario,
  ending_arr, ending_arr_recomputed, arr_reconciliation_diff, arr_reconciliation_ok_flag
from {{ ref('mart_arr_reconciliation_checks') }}
where arr_reconciliation_ok_flag = false
