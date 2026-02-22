-- Fails if ending_arr != starting_arr + new_arr + expansion_arr - contraction_arr - churn_arr (within tolerance).
-- Tolerance: 0.01 ARR to allow for floating point.

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
    ending_arr - (starting_arr + new_arr + expansion_arr - contraction_arr - churn_arr) as balance_diff
from {{ ref('mart_arr_waterfall_monthly') }}
where abs(ending_arr - (starting_arr + new_arr + expansion_arr - contraction_arr - churn_arr)) > 0.01
