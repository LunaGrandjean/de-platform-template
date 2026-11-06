-- Source: bronze_snapshot.product_reviews (111,322 rows, all strings).
-- FIXES: typing. review_id becomes INT64; p_id is trimmed.
-- LEFT ALONE: this is a bridge between products and reviews. It is 1:1 on the
--   review side today (111,322 distinct review_id), but a product has many
--   reviews, so joining it to sales fans rows out. Aggregate it first in gold.
select
    trim(p_id)                                             as p_id,
    safe_cast(review_id as int64)                          as review_id
from {{ source('bronze_snapshot', 'product_reviews') }}
