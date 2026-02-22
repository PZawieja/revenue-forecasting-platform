-- Fails if residual is large relative to mrr_delta (sanity: decomposition should explain most of the change).
-- Allow residual up to 2% of |mrr_delta| + 1 (to avoid div by zero and allow rounding).

select company_id, month, customer_id, product_family, mrr_delta, qty_effect, price_effect, residual
from {{ ref('mart_arr_decomposition_price_qty') }}
where prior_qty_total is not null
  and prior_avg_unit_price_effective is not null
  and abs(residual) > 0.02 * (abs(mrr_delta) + 1)
  and abs(mrr_delta) > 0.01
