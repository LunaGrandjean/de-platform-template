-- ONE PLACE where the legacy duplicate orders are resolved.
--
-- Every order in the snapshot exists twice under a different order_id. There is
-- no column that marks the copy, so the rule is inferred from the data:
--   * a twin shares buyer, payment and the exact order lines (product, quantity,
--     unit price) with the original;
--   * in all 110,940 plain pairs the lower order_id is also the earlier or equal
--     date, and the dates are 0 to 4 days apart;
--   * 185 groups hold 4, 6 or 8 orders, always an even number, spread over up to
--     966 days: a customer who bought the same basket several times, each
--     purchase duplicated.
-- So inside a (buyer, payment, basket) group the orders are sorted by
-- (order_date, order_id) and every odd rank is kept: the first purchase, the third,
-- and so on; the copy that follows each one is dropped.
--
-- Check: 222,644 orders must become exactly 111,322. If this ever changes, the
-- rule no longer matches the data and must be revisited, not patched.
-- Limit: this is an inference, not a marked fact. The twin could in principle be
-- the earlier row; the count and the date order make that unlikely.
with order_lines as (

    select
        order_id,
        string_agg(
            concat(p_id, ':', cast(quantity as string), ':', cast(unit_price_eur as string)),
            ',' order by p_id
        ) as basket
    from {{ ref('stg_order_items') }}
    group by order_id

),

ranked as (

    select
        orders.order_id,
        orders.buyer_id,
        orders.payment_id,
        orders.order_date,
        row_number() over (
            partition by orders.buyer_id, orders.payment_id, order_lines.basket
            order by orders.order_date, orders.order_id
        ) as rank_in_group
    from {{ ref('stg_orders') }} as orders
    inner join order_lines using (order_id)

)

select
    order_id,
    buyer_id,
    payment_id,
    order_date
from ranked
where mod(rank_in_group, 2) = 1
