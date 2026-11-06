-- The two facts describe the same sales at different grains, so their totals
-- must agree. This catches a join that repeats or drops rows between them.
-- Fails (returns a row) when units or gross revenue differ.
with items as (

    select sum(quantity) as units, sum(gross_amount_eur) as revenue
    from {{ ref('fct_order_items') }}

),

demand as (

    select sum(units_sold) as units, sum(gross_revenue_eur) as revenue
    from {{ ref('fct_daily_demand') }}

)

select items.units as item_units, demand.units as demand_units,
       items.revenue as item_revenue, demand.revenue as demand_revenue
from items
cross join demand
where items.units != demand.units
   or items.revenue != demand.revenue
