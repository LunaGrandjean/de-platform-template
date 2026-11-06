-- Source: bronze_snapshot.order_items (408,560 rows, all strings).
-- FIXES: typing. order_id and quantity become INT64, price NUMERIC (0 failed).
--   qty is renamed quantity and price_at_purchase unit_price_eur, so nobody
--   confuses a sold quantity with product.qty, which is current stock.
-- FLAGS: 30 lines are priced 0.00 (quantities 1 to 6). They are kept, because
--   nothing in the data says whether they are gifts or errors, and marked
--   is_zero_price so gold can decide how revenue treats them.
-- LEFT ALONE: price_at_purchase differs from the catalogue price on 299,632 of
--   the 408,560 lines. That is a historical price, not a defect.
select
    safe_cast(order_id as int64)                           as order_id,
    trim(p_id)                                             as p_id,
    safe_cast(qty as int64)                                as quantity,
    safe_cast(price_at_purchase as numeric)                as unit_price_eur,
    safe_cast(price_at_purchase as numeric) = 0            as is_zero_price
from {{ source('bronze_snapshot', 'order_items') }}
