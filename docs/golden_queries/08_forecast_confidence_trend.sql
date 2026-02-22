-- Forecast confidence score trend over the latest 12 months (base scenario).
-- Use to see whether confidence is improving or degrading over time.

with latest_12_months as (
  select distinct month
  from main.mart_executive_forecast_summary
  where scenario = 'base'
  order by month desc
  limit 12
)
select
  s.month,
  s.scenario,
  avg(s.avg_confidence_score) as avg_confidence_score,
  sum(s.total_forecast_revenue) as total_forecast_revenue,
  sum(s.total_actual_revenue)  as total_actual_revenue
from main.mart_executive_forecast_summary s
inner join latest_12_months l on l.month = s.month
where s.scenario = 'base'
group by s.month, s.scenario
order by s.month
