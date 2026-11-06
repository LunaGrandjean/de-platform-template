-- Guards the inferred twin rule in int_orders_deduplicated.
-- Every snapshot order has exactly one twin, so deduplication must keep exactly
-- half of them. If the count drifts, the rule no longer matches the data.
-- The test returns a row (and so fails) when the halves do not match.
with counts as (

    select
        (select count(*) from {{ ref('stg_orders') }})              as raw_orders,
        (select count(*) from {{ ref('int_orders_deduplicated') }}) as kept_orders

)

select *
from counts
where kept_orders * 2 != raw_orders
