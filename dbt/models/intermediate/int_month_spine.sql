with subs as (
    select
        min(contract_start_month) as min_month,
        max(contract_end_month) as max_month
    from {{ ref('stg_subscription_line_items') }}
),

pipeline as (
    select
        min(snapshot_month) as min_month,
        max(snapshot_month) as max_month
    from {{ ref('stg_pipeline_opportunities_snapshot') }}
),

bounds as (
    select
        least(s.min_month, p.min_month) as spine_start,
        (greatest(s.max_month, p.max_month) + (interval '1 month' * 6))::date as spine_end
    from subs s
    cross join pipeline p
),

month_offsets as (
    select unnest(generate_series(0, 119)) as month_offset
),

spine as (
    select
        (b.spine_start + (interval '1 month' * m.month_offset))::date as month
    from bounds b
    cross join month_offsets m
    where (b.spine_start + (interval '1 month' * m.month_offset))::date <= b.spine_end
)

select distinct month
from spine
