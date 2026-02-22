-- Fails if rank is not between 1 and 10 (sanity).

select company_id, month, segment, rank, customer_id
from {{ ref('mart_top_arr_movers') }}
where rank < 1 or rank > 10
