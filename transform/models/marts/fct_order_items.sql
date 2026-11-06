-- One row per order line of a real order.
-- Orders come from int_orders_deduplicated (the legacy twin of each order is
-- already gone) and each carries its buyer's single default-address country.
-- Zero-priced lines are kept: they count in units and add 0 to the amount.
-- gross_amount_eur = quantity x price at purchase, BEFORE any discount (every
-- discount_id is empty in the source) and with NO returns subtracted (the
-- returns table is empty). It is a gross figure, not net revenue.
select
    items.order_id,
    items.p_id,
    orders.order_date,
    orders.buyer_id,
    country.country_code,
    items.quantity,
    items.unit_price_eur,
    items.quantity * items.unit_price_eur as gross_amount_eur,
    items.is_zero_price
from {{ ref('stg_order_items') }} as items
inner join {{ ref('int_orders_deduplicated') }} as orders using (order_id)
left join {{ ref('int_buyer_country') }} as country
    on orders.buyer_id = country.customer_id
