-- What is forecast vs actual for the last 12 months (base scenario, aggregated)?
-- Use for planning and variance review.

with latest_12 as (
  select distinct month
  from main.mart_executive_forecast_summary
  where scenario = 'base'
  order by month desc
  limit 12
)
select
  s.month,
  s.scenario,
  sum(s.total_forecast_revenue) as total_forecast_revenue,
  sum(s.total_actual_revenue) as total_actual_revenue,
  sum(s.total_forecast_revenue) - sum(s.total_actual_revenue) as variance
from main.mart_executive_forecast_summary s
inner join latest_12 l on l.month = s.month
where s.scenario = 'base'
group by s.month, s.scenario
order by s.month
