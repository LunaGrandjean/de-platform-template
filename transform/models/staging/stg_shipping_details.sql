-- Source: bronze_snapshot.shipping_details (200,001 rows, all strings).
-- FIXES: country is a full English name. country_code adds the ISO 3166-1 alpha-2
--   code for the six markets present (Germany, France, Italy, Spain, Netherlands,
--   Belgium). Any other name maps to NULL, so a new country fails a test.
-- FIXES: typing. address_id becomes INT64.
-- LEFT ALONE: postal_code stays text. 8,290 postcodes have fewer than five
--   digits because they are Belgian (four digits); leading zeros are kept.
-- EXCLUDED, on purpose: street_address and phone. They are personal data, no
--   model needs them, and a column that is never selected cannot leak. Step 10
--   decides what silver may carry.
select
    safe_cast(address_id as int64)                         as address_id,
    trim(city)                                             as city,
    trim(state)                                            as region,
    trim(zip)                                              as postal_code,
    trim(country)                                          as country_name,
    case trim(country)
        when 'Germany'     then 'DE'
        when 'France'      then 'FR'
        when 'Italy'       then 'IT'
        when 'Spain'       then 'ES'
        when 'Netherlands' then 'NL'
        when 'Belgium'     then 'BE'
    end                                                    as country_code
from {{ source('bronze_snapshot', 'shipping_details') }}
