-- Provisional Step 4 performance aggregate, not the reconciled Lesson 5 fact.
-- A full valid-date range satisfies the base table's required partition filter.
-- Disable background refresh for this fixed snapshot; refresh explicitly after
-- a coordinated native-table rebuild (which can invalidate dependent views).
CREATE MATERIALIZED VIEW IF NOT EXISTS
  `data-engineering-i1.g07_warehouse.daily_demand`
PARTITION BY DATE_TRUNC(order_date, MONTH)
CLUSTER BY p_id
OPTIONS (enable_refresh = FALSE,
  description = 'Step 4 snapshot units by product/day; business duplicates retained')
AS
SELECT order_date, p_id, SUM(qty) AS units
FROM `data-engineering-i1.g07_warehouse.order_lines`
WHERE order_date >= DATE '0001-01-01'
GROUP BY order_date, p_id;
