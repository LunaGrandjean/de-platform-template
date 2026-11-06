-- Source: bronze_snapshot.customer_shipping (200,001 rows, all strings).
-- FIXES: is_default is the text '0' or '1'; it becomes a BOOLEAN.
-- FIXES: typing. address_id becomes INT64. c_id is renamed customer_id; it equals
--   orders.buyer_id for buyers (0 orders without a matching customer).
-- LEFT ALONE: a customer has two or three addresses. Exactly one is the default
--   for each of the 100,000 customers, and that rule is what picks the single
--   address in gold. Only the default is used, never all of them.
select
    safe_cast(address_id as int64)                         as address_id,
    trim(c_id)                                             as customer_id,
    is_default = '1'                                       as is_default
from {{ source('bronze_snapshot', 'customer_shipping') }}
