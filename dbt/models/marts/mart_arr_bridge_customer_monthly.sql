-- Customer-level ARR bridge for debugging the waterfall (mart_arr_waterfall_monthly).
-- Grain: company_id x month x customer_id x segment x scenario.
-- Same classification rules as mart_arr_waterfall_monthly. Base scenario from actuals;
-- upside/downside repeat same customer-level actuals (no customer-level forecast).
-- Deterministic order; no duplicates.

with scenarios as (
    select * from (values ('base'), ('upside'), ('downside')) as t(scenario)
),

-- Customer-level ARR by month (actuals): 12 * MRR
customer_arr_monthly as (
    select
        lim.company_id,
        lim.customer_id,
        c.segment,
        lim.month,
        12.0 * sum(lim.mrr) as arr
    from {{ ref('fct_subscription_line_item_monthly') }} lim
    inner join {{ ref('dim_customer') }} c on lim.company_id = c.company_id and lim.customer_id = c.customer_id
    group by lim.company_id, lim.customer_id, c.segment, lim.month
),

with_prior as (
    select
        curr.company_id,
        curr.customer_id,
        curr.segment,
        curr.month,
        coalesce(prev.arr, 0) as arr_prior_month,
        curr.arr as arr_current_month,
        curr.arr - coalesce(prev.arr, 0) as arr_delta
    from customer_arr_monthly curr
    left join customer_arr_monthly prev
        on prev.company_id = curr.company_id
        and prev.customer_id = curr.customer_id
        and prev.segment = curr.segment
        and prev.month = (curr.month - interval '1 month')::date
),

-- bridge_category: NEW, EXPANSION, CONTRACTION, CHURN, FLAT
classified as (
    select
        company_id,
        customer_id,
        segment,
        month,
        arr_prior_month,
        coalesce(arr_current_month, 0) as arr_current_month,
        arr_delta,
        case
            when arr_prior_month = 0 and coalesce(arr_current_month, 0) > 0 then 'NEW'
            when arr_prior_month > 0 and coalesce(arr_current_month, 0) = 0 then 'CHURN'
            when arr_prior_month > 0 and coalesce(arr_current_month, 0) > 0 and (coalesce(arr_current_month, 0) - arr_prior_month) > 0 then 'EXPANSION'
            when arr_prior_month > 0 and coalesce(arr_current_month, 0) > 0 and (coalesce(arr_current_month, 0) - arr_prior_month) < 0 then 'CONTRACTION'
            when arr_prior_month > 0 and coalesce(arr_current_month, 0) > 0 and (coalesce(arr_current_month, 0) - arr_prior_month) = 0 then 'FLAT'
            else 'FLAT'
        end as bridge_category
    from with_prior
),

base_rows as (
    select
        company_id,
        month,
        customer_id,
        segment,
        'base' as scenario,
        arr_prior_month,
        arr_current_month,
        arr_delta,
        bridge_category,
        (bridge_category = 'CHURN') as churn_flag,
        (bridge_category = 'NEW') as new_flag
    from classified
),

-- Repeat for upside/downside (same customer-level actuals; no customer-level forecast)
with_scenarios as (
    select
        b.company_id,
        b.month,
        b.customer_id,
        b.segment,
        s.scenario,
        b.arr_prior_month,
        b.arr_current_month,
        b.arr_delta,
        b.bridge_category,
        b.churn_flag,
        b.new_flag
    from base_rows b
    cross join scenarios s
)

select
    company_id,
    month,
    customer_id,
    segment,
    scenario,
    arr_prior_month,
    arr_current_month,
    arr_delta,
    bridge_category,
    churn_flag,
    new_flag
from with_scenarios
order by company_id, month, customer_id, segment, scenario
