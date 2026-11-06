-- Acceptance check: the agreed duplicate-order rule must keep meeting its own
-- assumption.
--
-- int_orders_deduplicated keeps every odd-ranked order of a (buyer, payment,
-- basket) group, which only works if each purchase appears exactly twice, so
-- every group holds an even number of orders. A group of 1 or 3 would be a
-- real single order or an ambiguous case that the rule would silently
-- mishandle. Such groups are returned here (and fail the test) instead of
-- being merged without notice.
with order_lines as (

    select
        order_id,
        string_agg(
            concat(p_id, ':', cast(quantity as string), ':', cast(unit_price_eur as string)),
            ',' order by p_id
        ) as basket
    from {{ ref('stg_order_items') }}
    group by order_id

)

select
    orders.buyer_id,
    orders.payment_id,
    order_lines.basket,
    count(*) as orders_in_group
from {{ ref('stg_orders') }} as orders
inner join order_lines using (order_id)
group by orders.buyer_id, orders.payment_id, order_lines.basket
having mod(count(*), 2) != 0
