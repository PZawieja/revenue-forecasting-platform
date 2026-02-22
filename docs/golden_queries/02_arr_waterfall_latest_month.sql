-- ARR waterfall for the latest available month (base scenario, All segments).
-- Use for board-level ARR storytelling and reconciliation.

with latest_month as (
  select max(month) as month from main.mart_arr_waterfall_monthly where scenario = 'base'
)
select
  w.month,
  w.scenario,
  sum(w.starting_arr)    as starting_arr,
  sum(w.new_arr)         as new_arr,
  sum(w.expansion_arr)   as expansion_arr,
  sum(w.contraction_arr) as contraction_arr,
  sum(w.churn_arr)       as churn_arr,
  sum(w.ending_arr)      as ending_arr,
  sum(w.net_new_arr)     as net_new_arr,
  (sum(w.starting_arr) + sum(w.expansion_arr) - sum(w.contraction_arr) - sum(w.churn_arr)) / nullif(sum(w.starting_arr), 0) as nrr,
  (sum(w.starting_arr) - sum(w.contraction_arr) - sum(w.churn_arr)) / nullif(sum(w.starting_arr), 0) as grr
from main.mart_arr_waterfall_monthly w
inner join latest_month l on l.month = w.month
where w.scenario = 'base'
group by w.month, w.scenario
order by w.month
