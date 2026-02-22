-- Pipeline coverage for the latest month: how much of the next 90-day forecast is covered by pipeline?
-- Use for new-business confidence and pipeline health.

with latest_month as (
  select max(month) as month from main.mart_forecast_coverage_metrics
)
select
  f.month,
  f.scenario,
  f.segment,
  avg(f.pipeline_coverage_ratio) as pipeline_coverage_ratio,
  avg(f.renewal_coverage_ratio) as renewal_coverage_ratio
from main.mart_forecast_coverage_metrics f
inner join latest_month l on l.month = f.month
where f.scenario = 'base'
group by f.month, f.scenario, f.segment
order by f.month, f.segment
