-- Fails if mrr or qty_total is negative (numeric sanity).

select company_id, month, customer_id, product_family, qty_total, mrr
from {{ ref('mart_arr_decomposition_price_qty') }}
where qty_total < 0 or mrr < 0
