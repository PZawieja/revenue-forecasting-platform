-- Segment- and stage-aware close probabilities from config. Grain: company_id x segment x stage.
select
    company_id,
    segment,
    stage,
    cast(p_base as double) as p_base,
    cast(p_upside as double) as p_upside,
    cast(p_downside as double) as p_downside
from {{ ref('stage_probability_config') }}
