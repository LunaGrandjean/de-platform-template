-- Source: bronze_snapshot.review (111,322 rows, all strings).
-- FIXES: blank review text becomes NULL: 14 empty strings and 110 single spaces
--   (124 in all). Text that is not blank is returned exactly as written.
-- FIXES: typing. review_id and rating become INT64 (0 failed, all ratings 1-5).
-- LEFT ALONE: 24 reviews read "NA", "N/A", "None" and similar. A customer can
--   write those words, and nothing separates that from a missing value, so they
--   stay as text.
-- LEFT ALONE: seller_product_flag is 'S' on all 111,322 rows. It carries no
--   information today; kept so the source contract is complete.
select
    safe_cast(review_id as int64)                          as review_id,
    nullif(trim(buyer_id), '')                             as buyer_id,
    if(trim(r_desc) = '', null, r_desc)                    as review_text,
    title                                                  as title,
    safe_cast(rating as int64)                             as rating,
    seller_product_flag                                    as seller_product_flag
from {{ source('bronze_snapshot', 'review') }}
