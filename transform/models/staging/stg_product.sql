-- Source: bronze_snapshot.product (42,858 rows, all strings).
-- FIXES: 6 products have an empty p_name; it becomes NULL, so "every product has
--   a name" is a test that can fail instead of a blank that passes.
-- FIXES: typing. price NUMERIC, qty INT64, category_id INT64 (0 failed).
--   qty is renamed stock_qty: it is current stock, not a sold quantity.
-- LEFT ALONE: p_desc is JSON stored as text. Nothing downstream reads it, so it
--   is not parsed here.
select
    trim(p_id)                                             as p_id,
    nullif(trim(p_name), '')                               as p_name,
    p_desc                                                 as p_desc,
    safe_cast(price as numeric)                            as price_eur,
    safe_cast(qty as int64)                                as stock_qty,
    safe_cast(nullif(trim(category_id), '') as int64)      as category_id
from {{ source('bronze_snapshot', 'product') }}
