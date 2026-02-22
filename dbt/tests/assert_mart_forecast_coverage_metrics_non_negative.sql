-- Fails if any ratio is negative (sanity).

select company_id, month, segment, scenario,
  pipeline_coverage_ratio, renewal_coverage_ratio, concentration_ratio_top5
from {{ ref('mart_forecast_coverage_metrics') }}
where pipeline_coverage_ratio < 0
   or renewal_coverage_ratio < 0
   or concentration_ratio_top5 < 0
