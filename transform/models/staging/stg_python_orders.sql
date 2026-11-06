-- Source: bronze_ingestion.python_orders (cursor extract of public.orders).
-- FIXES: BigQuery refuses to read this table without a filter on ingestion_date;
--   the full-range filter below satisfies it, so downstream models do not each
--   have to repeat it.
-- LEFT ALONE: it is already typed. It contains every snapshot order_id plus
--   newer ones, so it is never summed with stg_orders. Which of the two feeds
--   gold is a decision for deliverable 4.
select
    order_id,
    nullif(trim(buyer_id), '')                             as buyer_id,
    discount_id,
    payment_id,
    order_date,
    ingestion_date
from {{ source('bronze_ingestion', 'python_orders') }}
where ingestion_date >= date '2000-01-01'
