-- Source: bronze_ingestion.kafka_clickstream (29,144 rows).
-- FIXES: the same event_id arrives more than once (29,144 rows, 18,276 distinct
--   event_id), each copy at a different Kafka position. One row is kept per
--   event_id, the earliest by position, so counts of views and carts are not
--   inflated.
-- FIXES: typing. Timestamps arrive as ISO strings (0 failed) and become
--   TIMESTAMP. event_at is when it happened on the generator clock;
--   ingested_at is when it was loaded. They differ by weeks, so freshness uses
--   ingested_at.
-- EXCLUDED: raw_value and the raw key. The parsed columns carry what is needed.
-- NOTE: the full-range event_date filter satisfies the BigQuery partition rule.
select
    event_id,
    event_type,
    safe_cast(event_time as timestamp)                     as event_at,
    safe_cast(emitted_at as timestamp)                     as emitted_at,
    buyer_id,
    session_id,
    safe_cast(_ingested_at as timestamp)                   as ingested_at,
    event_date,
    _kafka_topic                                           as kafka_topic,
    _kafka_partition                                       as kafka_partition,
    _kafka_offset                                          as kafka_offset
from {{ source('bronze_ingestion', 'kafka_clickstream') }}
where event_date >= date '2000-01-01'
qualify row_number() over (
    partition by event_id
    order by _kafka_partition, _kafka_offset
) = 1
