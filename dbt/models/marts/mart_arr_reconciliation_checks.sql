-- ARR reconciliation (trust/audit): validate ending_arr = starting_arr + new_arr + expansion_arr - contraction_arr - churn_arr.
-- Grain: company_id x month x segment x scenario.
-- Fails audit when arr_reconciliation_ok_flag is false (abs(diff) > 0.01).

with w as (
    select
        company_id,
        month,
        segment,
        scenario,
        starting_arr,
        new_arr,
        expansion_arr,
        contraction_arr,
        churn_arr,
        ending_arr
    from {{ ref('mart_arr_waterfall_monthly') }}
)

select
    company_id,
    month,
    segment,
    scenario,
    starting_arr,
    new_arr,
    expansion_arr,
    contraction_arr,
    churn_arr,
    ending_arr,
    starting_arr + new_arr + expansion_arr - contraction_arr - churn_arr as ending_arr_recomputed,
    ending_arr - (starting_arr + new_arr + expansion_arr - contraction_arr - churn_arr) as arr_reconciliation_diff,
    abs(ending_arr - (starting_arr + new_arr + expansion_arr - contraction_arr - churn_arr)) <= 0.01 as arr_reconciliation_ok_flag
from w
