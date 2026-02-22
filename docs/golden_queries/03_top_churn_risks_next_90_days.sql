-- Who is at highest churn risk in the latest month (top 20 by risk)?
-- Use for renewal prioritization and CSM focus. "Next 90 days" = watchlist for latest month.

with latest_month as (
  select max(month) as month from main.mart_churn_risk_watchlist
)
select
  w.month,
  coalesce(c.customer_name, w.customer_id::varchar) as customer_name,
  w.segment,
  w.months_to_renewal,
  w.current_arr,
  w.p_renew,
  w.health_score_1_10,
  w.slope_bucket,
  w.risk_reason
from main.mart_churn_risk_watchlist w
left join main.dim_customer c on c.company_id = w.company_id and c.customer_id = w.customer_id
inner join latest_month l on l.month = w.month
order by w.risk_rank
limit 20
