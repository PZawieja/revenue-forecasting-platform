-- ML feature table for pipeline model. Grain: company_id x opportunity_id x snapshot_month.
-- Stable training data: snapshot-level features + label (closed_won_flag from later snapshots).
-- closed_won_flag = 1 if opportunity ever reaches 'closed won' in a later snapshot; 0 if 'closed lost' or still open (simplification: no cutoff window; open = 0).
-- Materialized as table for fast Python reads.

{{ config(materialized='table') }}

with pipe as (
    select
        company_id,
        opportunity_id,
        customer_id,
        snapshot_month,
        segment,
        stage,
        amount,
        expected_close_month,
        opportunity_type
    from {{ ref('stg_pipeline_opportunities_snapshot') }}
),

first_seen as (
    select
        company_id,
        opportunity_id,
        min(snapshot_month) as first_snapshot_month
    from pipe
    group by company_id, opportunity_id
),

later_closed_won as (
    select
        p.company_id,
        p.opportunity_id,
        p.snapshot_month,
        max(case when p2.stage = 'closed won' then 1 else 0 end) as ever_closed_won
    from pipe p
    left join pipe p2
        on p2.company_id = p.company_id
        and p2.opportunity_id = p.opportunity_id
        and p2.snapshot_month > p.snapshot_month
    group by p.company_id, p.opportunity_id, p.snapshot_month
),

closed_won_flag as (
    select
        company_id,
        opportunity_id,
        snapshot_month,
        case when coalesce(ever_closed_won, 0) = 1 then 1 else 0 end as closed_won_flag
    from later_closed_won
)

select
    p.company_id,
    p.opportunity_id,
    p.snapshot_month,
    p.segment,
    p.stage,
    p.amount,
    p.expected_close_month,
    p.opportunity_type,
    date_diff('month', f.first_snapshot_month, p.snapshot_month) as deal_age_months,
    c.closed_won_flag
from pipe p
inner join first_seen f
    on f.company_id = p.company_id and f.opportunity_id = p.opportunity_id
left join closed_won_flag c
    on c.company_id = p.company_id and c.opportunity_id = p.opportunity_id and c.snapshot_month = p.snapshot_month
