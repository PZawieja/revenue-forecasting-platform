-- Joined ML dataset: features + labels for renewal model.
-- Grain: (company_id, customer_id, renewal_month). Includes as_of_month and label_renewed (1/0).
-- Multi-company, time-valid; for training only (labels require observed post-renewal months).

with features as (
    select * from {{ ref('ml_features__renewal') }}
),

labels as (
    select * from {{ ref('ml_labels__renewal') }}
)

select
    f.company_id,
    f.customer_id,
    f.renewal_month,
    f.as_of_month,
    f.segment,
    f.segment_group,
    f.current_mrr_pre_renewal,
    f.health_score_1_10,
    f.usage_per_user_total,
    f.trailing_3m_avg_usage_per_user_total,
    f.trailing_3m_slope_bucket,
    f.months_to_renewal,
    l.label_renewed
from features f
inner join labels l
    on l.company_id = f.company_id
    and l.customer_id = f.customer_id
    and l.renewal_month = f.renewal_month
