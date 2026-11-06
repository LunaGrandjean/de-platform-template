-- One row per market. Built from the addresses, so a country appears here only
-- if a customer lives in it.
select distinct
    country_code,
    country_name
from {{ ref('stg_shipping_details') }}
where country_code is not null
