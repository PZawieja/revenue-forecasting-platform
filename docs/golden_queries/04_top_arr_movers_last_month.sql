-- Top ARR movers (growth or decline) in the latest month.
-- Use for account-level action and understanding who drove bridge.

with latest_month as (
  select max(month) as month from main.mart_top_arr_movers
)
select
  m.month,
  m.customer_name,
  m.arr_delta,
  m.bridge_category,
  m.health_score_1_10,
  m.slope_bucket,
  m.rank
from main.mart_top_arr_movers m
inner join latest_month l on l.month = m.month
order by m.rank
limit 10
