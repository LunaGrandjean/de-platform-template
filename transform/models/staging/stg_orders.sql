-- Source: bronze_snapshot.orders (222,644 rows, all strings).
-- FIXES: typing. order_id and payment_id become INT64, order_date a DATE
--   (0 values failed to parse). buyer_id is trimmed.
-- FIXES: discount_id is blank on all 222,644 rows; it is read as a NULL INT64 so
--   the column stays in the contract and a test can fail the day a value appears.
-- LEFT ALONE, on purpose: every order appears twice under a different order_id
--   (110,940 pairs plus 185 groups of more than two, 0 single orders, grouped by
--   buyer + payment + identical order lines, dates 0 to 4 days apart). Spotting a
--   twin needs the order lines, so it is resolved once, in the intermediate
--   model of deliverable 4, not here.
-- NOTE: the legacy unnamed index column is never declared on the external
--   table (BigQuery rejects an empty field name), so there is nothing to drop.
select
    safe_cast(order_id as int64)                           as order_id,
    nullif(trim(buyer_id), '')                             as buyer_id,
    safe_cast(nullif(trim(discount_id), '') as int64)      as discount_id,
    safe_cast(nullif(trim(payment_id), '') as int64)       as payment_id,
    safe_cast(order_date as date)                          as order_date
from {{ source('bronze_snapshot', 'orders') }}
