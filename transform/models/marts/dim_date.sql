-- One row per calendar day from 2023-01-01 to 2026-12-31, whether or not
-- anything was sold. Orders run 2023-09-15 to 2026-09-14, so the range covers
-- every order date with room before and after.
with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-01-01' as date)",
        end_date="cast('2027-01-01' as date)"
    ) }}

)

select
    cast(date_day as date)                          as date_day,
    extract(year from date_day)                     as year,
    extract(month from date_day)                    as month,
    extract(day from date_day)                      as day_of_month,
    extract(isoweek from date_day)                  as iso_week,
    extract(isoyear from date_day)                  as iso_year,
    extract(dayofweek from date_day)                as day_of_week,
    extract(dayofweek from date_day) in (1, 7)      as is_weekend
from spine
