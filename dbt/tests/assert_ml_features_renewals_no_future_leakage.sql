-- Data contract / leakage check: renewal_month must not be in the future relative to actuals.
-- Label renewed_flag is derived from MRR in (renewal_month + 1 month); we can only have labels when that month exists.
-- Fails if any row has renewal_month > max(month) - 1 month in fct_subscription_line_item_monthly.
select
  f.company_id,
  f.customer_id,
  f.renewal_month,
  m.max_month
from {{ ref('ml_features_renewals') }} f
cross join (select max(month) as max_month from {{ ref('fct_subscription_line_item_monthly') }}) m
where f.renewal_month > (m.max_month - interval '1 month')::date
