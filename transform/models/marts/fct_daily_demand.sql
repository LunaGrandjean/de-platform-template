-- One row per product, per country, per day on which it sold.
-- Days with no sale have no row (they are not stored as zeros). Built only from
-- fct_order_items, so the deduplication and the country rule are applied once,
-- upstream, and never re-derived here.
select
    p_id,
    country_code,
    order_date,
    sum(quantity)                   as units_sold,
    count(distinct order_id)        as orders,
    sum(gross_amount_eur)           as gross_revenue_eur
from {{ ref('fct_order_items') }}
group by p_id, country_code, order_date
