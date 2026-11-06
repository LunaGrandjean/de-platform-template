-- One row per catalogue product, with its review summary.
-- Reviews are aggregated to one row per product in int_product_review_stats
-- first, so this join cannot repeat a product. A product with no review has
-- review_count 0 and a NULL avg_rating.
select
    product.p_id,
    product.p_name,
    product.price_eur,
    product.stock_qty,
    product.category_id,
    coalesce(stats.review_count, 0) as review_count,
    stats.avg_rating
from {{ ref('stg_product') }} as product
left join {{ ref('int_product_review_stats') }} as stats using (p_id)
