-- Resolves the fan-out from buyers to addresses.
--
-- A customer has two or three addresses, so joining orders to addresses directly
-- would multiply every order. The data carries a rule that picks one: each of the
-- 100,000 customers has exactly one is_default address. Only that address
-- is used, so this model has exactly one row per customer_id.
select
    shipping.customer_id,
    details.country_code,
    details.country_name
from {{ ref('stg_customer_shipping') }} as shipping
inner join {{ ref('stg_shipping_details') }} as details using (address_id)
where shipping.is_default
