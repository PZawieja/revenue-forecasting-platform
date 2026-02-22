with usage_agg as (
    select
        company_id,
        customer_id,
        month,
        sum(usage_count) as usage_total,
        sum(active_users) as active_users_total,
        sum(usage_count) / nullif(sum(active_users), 0) as usage_per_user_total
    from {{ ref('stg_usage_monthly') }}
    group by company_id, customer_id, month
),

p90_by_month as (
    select
        company_id,
        month,
        quantile_cont(0.9) within group (order by usage_per_user_total) as p90_usage_per_user_total
    from usage_agg
    group by company_id, month
),

with_trend as (
    select
        u.company_id,
        u.customer_id,
        u.month,
        u.usage_total,
        u.active_users_total,
        u.usage_per_user_total,
        avg(u.usage_per_user_total) over (
            partition by u.company_id, u.customer_id
            order by u.month
            rows between 2 preceding and current row
        ) as trailing_3m_avg_usage_per_user_total,
        case
            when lag(u.usage_per_user_total, 2) over (partition by u.company_id, u.customer_id order by u.month) is null then 'flat'
            when u.usage_per_user_total >= lag(u.usage_per_user_total, 2) over (partition by u.company_id, u.customer_id order by u.month) * 1.05 then 'growing'
            when u.usage_per_user_total <= lag(u.usage_per_user_total, 2) over (partition by u.company_id, u.customer_id order by u.month) * 0.95 then 'declining'
            else 'flat'
        end as trailing_3m_slope_bucket
    from usage_agg u
),

with_p90 as (
    select
        t.*,
        p.p90_usage_per_user_total
    from with_trend t
    left join p90_by_month p on t.company_id = p.company_id and t.month = p.month
),

with_customers as (
    select
        w.company_id,
        w.month,
        w.customer_id,
        c.segment,
        c.segment_group,
        c.crm_health_input,
        w.usage_total,
        w.active_users_total,
        w.usage_per_user_total,
        w.trailing_3m_avg_usage_per_user_total,
        w.trailing_3m_slope_bucket,
        hw.weight_crm,
        hw.weight_usage,
        hw.weight_trend,
        coalesce(c.crm_health_input / 10.0, hw.default_crm_if_null) as crm_score_norm,
        least(1.0, w.usage_per_user_total / nullif(w.p90_usage_per_user_total, 0)) as usage_score_norm,
        case w.trailing_3m_slope_bucket
            when 'declining' then 0.2
            when 'flat' then 0.5
            when 'growing' then 0.8
            else 0.5
        end as trend_score_norm
    from with_p90 w
    inner join {{ ref('stg_customers') }} c on w.company_id = c.company_id and w.customer_id = c.customer_id
    inner join {{ ref('health_weights_config') }} hw on hw.company_id = c.company_id and hw.segment_group = c.segment_group
),

weighted as (
    select
        *,
        least(1.0, greatest(0.0,
            crm_score_norm * weight_crm + usage_score_norm * weight_usage + trend_score_norm * weight_trend
        )) as health_raw
    from with_customers
)

select
    company_id,
    month,
    customer_id,
    segment,
    segment_group,
    crm_health_input,
    usage_total,
    active_users_total,
    usage_per_user_total,
    trailing_3m_avg_usage_per_user_total,
    trailing_3m_slope_bucket,
    health_raw,
    1 + floor(9 * health_raw)::integer as health_score_1_10
from weighted
