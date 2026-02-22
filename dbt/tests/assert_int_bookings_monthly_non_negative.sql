-- Fails if any bookings_mrr_equivalent or bookings_arr is negative.

select company_id, month, customer_id, segment, bookings_mrr_equivalent, bookings_arr
from {{ ref('int_bookings_monthly') }}
where bookings_mrr_equivalent < 0 or bookings_arr < 0
