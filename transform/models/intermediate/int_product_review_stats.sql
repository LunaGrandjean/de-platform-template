-- Resolves the fan-out from products to reviews.
--
-- A product has many reviews and the data holds no rule that picks one, so the
-- reviews cannot be joined row for row. They are aggregated to one row per
-- product here, before anything joins on p_id, so no sales row is ever repeated.
select
    links.p_id,
    count(*)                       as review_count,
    countif(reviews.rating is not null) as rated_review_count,
    round(avg(reviews.rating), 2)  as avg_rating
from {{ ref('stg_product_reviews') }} as links
inner join {{ ref('stg_review') }} as reviews using (review_id)
group by links.p_id
