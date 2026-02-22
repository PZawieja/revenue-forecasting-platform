-- Fails if any (company_id, month, segment) does not have exactly 3 scenarios (base, upside, downside).

select company_id, month, segment, count(*) as scenario_count
from {{ ref('mart_forecast_coverage_metrics') }}
group by company_id, month, segment
having count(*) <> 3
