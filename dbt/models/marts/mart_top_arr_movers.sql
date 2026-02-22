-- Top 10 customers by absolute ARR delta vs prior month, per month and segment (executive view).
-- Grain: company_id x month x segment x rank (rank 1..10). One row per customer per month (per segment).
-- Uses mart_arr_bridge_customer_monthly (base scenario), dim_customer (name), int_customer_health_monthly (health, slope).

with bridge_base as (
    select
        company_id,
        month,
        customer_id,
        segment,
        arr_delta,
        bridge_category
    from {{ ref('mart_arr_bridge_customer_monthly') }}
    where scenario = 'base'
),

customers as (
    select company_id, customer_id, customer_name, segment from {{ ref('dim_customer') }}
),

health as (
    select
        company_id,
        month,
        customer_id,
        health_score_1_10,
        trailing_3m_slope_bucket as slope_bucket
    from {{ ref('int_customer_health_monthly') }}
),

with_health as (
    select
        b.company_id,
        b.month,
        b.customer_id,
        c.customer_name,
        b.segment,
        b.arr_delta,
        b.bridge_category,
        h.health_score_1_10,
        h.slope_bucket
    from bridge_base b
    inner join customers c on c.company_id = b.company_id and c.customer_id = b.customer_id
    left join health h on h.company_id = b.company_id and h.customer_id = b.customer_id and h.month = b.month
),

ranked as (
    select
        company_id,
        month,
        segment,
        customer_id,
        customer_name,
        arr_delta,
        bridge_category,
        health_score_1_10,
        slope_bucket,
        row_number() over (
            partition by company_id, month, segment
            order by abs(arr_delta) desc, customer_id
        ) as rank
    from with_health
)

select
    company_id,
    month,
    segment,
    rank,
    customer_id,
    customer_name,
    arr_delta,
    bridge_category,
    health_score_1_10,
    slope_bucket
from ranked
where rank <= 10
order by company_id, month, segment, rank
