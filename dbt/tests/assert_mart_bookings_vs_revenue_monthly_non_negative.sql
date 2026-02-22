-- Fails if any bookings_arr or recognized_arr is negative.

select company_id, month, segment, bookings_arr, recognized_arr
from {{ ref('mart_bookings_vs_revenue_monthly') }}
where bookings_arr < 0 or recognized_arr < 0
