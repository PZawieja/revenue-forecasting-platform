-- Fails if current_arr is negative (sanity).

select company_id, month, customer_id, current_arr
from {{ ref('mart_churn_risk_watchlist') }}
where current_arr < 0
