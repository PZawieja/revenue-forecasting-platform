-- Price vs quantity decomposition of ARR changes for existing customers.
-- Grain: company_id x month x customer_id x product_family.
-- Uses fct_subscription_line_item_monthly; product_family from dim_product.
-- Decomposition: mrr_delta ≈ qty_effect + price_effect; residual = mrr_delta - qty_effect - price_effect.
-- Residual can be non-zero due to rounding and because we use quantity-weighted average unit price
-- within product_family (multiple lines/contracts aggregate to one avg price per period, so the
-- discrete P*Q vs (avg P)*(sum Q) can differ slightly).

with line_items as (
    select
        lim.company_id,
        lim.customer_id,
        p.product_family,
        lim.month,
        lim.quantity,
        lim.unit_price,
        lim.discount_pct,
        lim.mrr
    from {{ ref('fct_subscription_line_item_monthly') }} lim
    inner join {{ ref('dim_product') }} p on p.company_id = lim.company_id and p.product_id = lim.product_id
),

-- Per month, customer, product_family: qty_total, avg_unit_price_effective, mrr
current_agg as (
    select
        company_id,
        customer_id,
        product_family,
        month,
        sum(quantity) as qty_total,
        sum(mrr) / nullif(sum(quantity), 0) as avg_unit_price_effective,
        sum(mrr) as mrr
    from line_items
    group by company_id, customer_id, product_family, month
),

-- Prior month same customer + product_family
with_prior as (
    select
        curr.company_id,
        curr.customer_id,
        curr.product_family,
        curr.month,
        curr.qty_total,
        curr.avg_unit_price_effective,
        curr.mrr,
        prev.qty_total as prior_qty_total,
        prev.avg_unit_price_effective as prior_avg_unit_price_effective,
        prev.mrr as prior_mrr
    from current_agg curr
    left join current_agg prev
        on prev.company_id = curr.company_id
        and prev.customer_id = curr.customer_id
        and prev.product_family = curr.product_family
        and prev.month = (curr.month - interval '1 month')::date
),

decomposed as (
    select
        company_id,
        customer_id,
        product_family,
        month,
        qty_total,
        avg_unit_price_effective,
        mrr,
        prior_qty_total,
        prior_avg_unit_price_effective,
        prior_mrr,
        qty_total - coalesce(prior_qty_total, 0) as qty_delta,
        avg_unit_price_effective - coalesce(prior_avg_unit_price_effective, 0) as price_delta,
        mrr - coalesce(prior_mrr, 0) as mrr_delta,
        (qty_total - coalesce(prior_qty_total, 0)) * coalesce(prior_avg_unit_price_effective, 0) as qty_effect,
        (avg_unit_price_effective - coalesce(prior_avg_unit_price_effective, 0)) * qty_total as price_effect
    from with_prior
)

select
    company_id,
    month,
    customer_id,
    product_family,
    qty_total,
    avg_unit_price_effective,
    mrr,
    prior_qty_total,
    prior_avg_unit_price_effective,
    qty_delta,
    price_delta,
    mrr_delta,
    qty_effect,
    price_effect,
    mrr_delta - qty_effect - price_effect as residual
from decomposed
