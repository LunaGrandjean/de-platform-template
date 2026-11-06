# Acceptance criteria

What must be true for the warehouse to be usable, and the test that checks it.
The criteria come from the "Acceptance checks for Lesson 5" in
[the star design](model/star-daily-demand.md), plus the course's standard
checks. Every test runs in `dbt build`, except where the table says otherwise.

```bash
cd transform
uv run --locked dbt deps --profiles-dir .
uv run --locked dbt build --profiles-dir .
uv run --locked dbt source freshness --profiles-dir .   # freshness is a separate command
```

Last full run, 2026-11-06: 86 items, 85 pass, 1 warns (B5, a known source
defect), 0 fail. Source freshness at the same time: see C1 and C2.

Test files: [`marts.yml`](../transform/models/marts/marts.yml),
[`intermediate.yml`](../transform/models/intermediate/intermediate.yml),
[`staging.yml`](../transform/models/staging/staging.yml),
[`sources.yml`](../transform/models/sources.yml) and the singular tests in
[`transform/tests/`](../transform/tests/).

## A. The daily-demand star

| # | Criterion | Test | Status |
|---|---|---|---|
| A1 | The fact key is unique and every dimension key is unique. | `dbt_utils.unique_combination_of_columns` on `fct_daily_demand` (`p_id`, `country_code`, `order_date`) and on `fct_order_items` (`order_id`, `p_id`); `unique` on `dim_product.p_id`, `dim_country.country_code`, `dim_date.date_day`. | Tested, passes |
| A2 | Every fact row resolves to a date, a product and a country. | `relationships` from both facts to `dim_date`, `dim_product` and `dim_country`, plus `not_null` on each key. | Tested, passes. Differs from the design: see D2. |
| A3 | The country join keeps the number of order lines and their quantities. | [`assert_reconciled_lines_preserved`](../transform/tests/assert_reconciled_lines_preserved.sql): reconciled silver lines against `fct_order_items`, both counted. | Tested, passes |
| A4 | `units_sold` is a positive integer on every stored row. | `dbt_utils.accepted_range` (min 1) on `fct_daily_demand.units_sold`; the column is `INT64`. | Tested, passes |
| A5 | Summed fact quantities equal the reconciled silver quantities. | Same test as A3, also against `fct_daily_demand`; and [`assert_daily_demand_matches_order_items`](../transform/tests/assert_daily_demand_matches_order_items.sql) for units and gross revenue between the two facts. | Tested, passes (693,320 units) |
| A6 | Replaying the same input produces the same keys and quantities. | **Not a dbt test.** Checked by hand with an order-independent fingerprint of the table taken before and after a full rebuild: 203,431 rows, 693,320 units, identical fingerprint both times. The snapshot is a frozen load (`20261026T105159Z-2efcb86a`), which is what makes this repeatable. | Checked once, not automated. Why: dbt compares one run to itself; a replay check needs two runs. |
| A7 | The duplicate-order rule is exercised, including genuine repeat purchases. | [`assert_dedup_halves_orders`](../transform/tests/assert_dedup_halves_orders.sql): 222,644 orders must become exactly half. [`assert_twin_groups_are_even`](../transform/tests/assert_twin_groups_are_even.sql): every buyer + payment + basket group holds an even number of orders, which is the rule's assumption. `unique` on `int_orders_deduplicated.order_id`. The 185 groups of 4, 6 or 8 orders are the repeat purchases (up to 966 days apart). | Tested, passes. 0 unresolved cases today. |
| A8 | A changed default address exposes the country-restatement limit. | **Cannot be a test.** The source does not store the delivery address of an order, so no query can tell whether the country was true at the time. The limitation is stated in the design and in D3. | Not testable, documented |

## B. Source and model quality

| # | Criterion | Test | Status |
|---|---|---|---|
| B1 | Gross revenue is not double-counted. | The twins are removed once, in `int_orders_deduplicated`, and A1, A3, A5 and A7 guard it. Without the rule revenue would read 39,199,900.62 EUR; with it, 19,599,950.31 EUR. | Tested, passes. Definition agreed: see D1. |
| B2 | Every order line has a product. | `relationships` from `fct_order_items.p_id` and `fct_daily_demand.p_id` to `dim_product`. | Tested, passes |
| B3 | Every order has a buyer with exactly one country. | `unique` and `not_null` on `int_buyer_country.customer_id` and `country_code`; `not_null` on `fct_order_items.country_code`. Proven to fail: removing the `is_default` rule produced 66,781 duplicated customers and stopped the build before the facts. | Tested, passes |
| B4 | Ratings are between 1 and 5. | `dbt_utils.accepted_range` (1 to 5) on `stg_review.rating`. | Tested, passes |
| B5 | Every product has a name. | `not_null` on `dim_product.p_name`, set to warn up to 6 and fail at 7 or more. The source has 6 products with an empty name, which staging turns into NULL. | **Warns (6)**. This is a real source defect, deliberately not hidden. |
| B6 | Prices and quantities are valid. | `accepted_range` (quantity at least 1, unit price at least 0) on `fct_order_items`. 15 lines are priced 0.00 and kept, flagged `is_zero_price`. | Tested, passes |
| B7 | Clickstream has no repeated event. | `unique` on `stg_kafka_clickstream.event_id`. The source holds 29,144 rows for 18,276 events; staging keeps one per `event_id`. | Tested, passes |
| B8 | A new country does not slip in unmapped. | `accepted_values` and `not_null` on `stg_shipping_details.country_code`. | Tested, passes |

## C. Freshness

| # | Criterion | Test | Status on 2026-11-06 |
|---|---|---|---|
| C1 | Orders data is no more than 36 hours old (warn) or 72 hours (error). | `dbt source freshness`, `bronze_ingestion.python_orders`, thresholds explained in [`sources.yml`](../transform/models/sources.yml). | **Warn**, 45.7 h. The extractor is run by hand. |
| C2 | Clickstream is no more than 6 hours old (warn) or 24 hours (error), measured on load time. | `dbt source freshness`, `bronze_ingestion.kafka_clickstream`. | **Error**, 32.1 h. Everything was loaded once on 2026-11-05. |
| C3 | The frozen snapshot has no freshness rule. | None, on purpose. It has no load-time column, and its `load_id` is its version. | Not testable, documented |

C1 and C2 are alerts that are true today, not thresholds bent to pass. They go
green when the extractor and consumer run on a schedule (Step 7).

## D. Where the build departs from the design sketch

These are decisions to confirm, not hidden gaps.

1. **Revenue (agreed 2026-11-06).** The design said not to introduce revenue
   until price, discount and returns were defined. The group agreed a *gross*
   figure: `gross_amount_eur` and `gross_revenue_eur` are quantity times price at
   purchase, before discounts, without returns. That is unambiguous today because
   every `discount_id` is empty and the returns table is empty. It is not net
   revenue, and it must be revisited if discounts or returns start to carry data.
2. **No UNKNOWN country member.** The design routes unmatched buyers to an
   `UNKNOWN` member. Here a missing country fails `not_null` instead, which is
   stricter and currently never triggers (all 100,000 customers have exactly one
   default address). Add the member if the data can start to contain new
   sign-ups with no address.
3. **Country is a proxy.** It is the buyer's default-address country, not the
   delivery country. The mapping is rebuilt at each run, not frozen, so a later
   snapshot can restate history.
4. **Natural keys, no surrogate keys.** Facts join on `p_id`, `country_code`
   and `date_day`, not on `product_key`, `country_key` and `date_key`. The
   grains are unchanged.
5. **Input.** The design names the connector stream as the sales input and
   excludes the custom extractor. The group took the Python route instead, and
   gold reads the Step 2 snapshot only. `python_orders` has no order lines, so
   it cannot feed the facts yet.
