-- Fails if any (month, segment) does not have exactly 3 scenarios (base, upside, downside).
select
    month,
    segment,
    count(distinct scenario) as scenario_count
from {{ ref('fct_revenue_forecast_monthly') }}
group by month, segment
having count(distinct scenario) <> 3
