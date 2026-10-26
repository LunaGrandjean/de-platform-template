# Bronze data dictionary

The initial CSV snapshot was verified on 2026-10-26 for load `20261026T105159Z-2efcb86a` in
`gs://data-engineering-i1-marketplace-group`, region `europe-west1`.
The [manifest](evidence/step2/landing-manifest.json) lists actual columns, row
counts and hashes. The SQL types below describe the operational schema; **every
landed CSV field uses Arrow `string`**, encoded in Parquet as `BYTE_ARRAY` with
a string annotation, to preserve its lexical value. Empty fields remain empty
strings; literal `NA`, `NULL`, `None` and
leading zeros remain text. No columns are dropped or renamed.

## Classification rules

The dataset's customer details are synthetic, while product/review text and
pseudonymous buyer IDs originate from a public dataset. Apply personal-data
controls to the exercise as if the data were operational. `Yes` identifies
direct or linkable identifiers and address information; `Possible` identifies
uncontrolled free text. `No` means no direct personal information in that field
by design, not that the surrounding row can be shared without restriction.
For example, a quantity linked to a buyer remains part of their purchase history.

The column shown as `""` is an **actual empty CSV header**, often displayed as
`Unnamed: 0` by pandas. It is retained verbatim as a string field in Parquet.
It has no business-key meaning. `ingestion_date` and `load_id` are directory
metadata, not additional columns inside each file.

## `orders` — 222,644 rows

One operational order record; business duplicates with different IDs remain.

| Column | Source SQL type | Meaning | Personal data? |
|---|---|---|---|
| `""` | Export-only index | Legacy CSV row index | No |
| `order_id` | INTEGER | Operational order identifier; joins order lines | Yes: linkable transaction ID |
| `buyer_id` | VARCHAR(40), nullable | Buyer reference | Yes: pseudonymous buyer ID |
| `discount_id` | INTEGER, nullable | Optional discount reference | No |
| `payment_id` | INTEGER, nullable | Saved-payment reference; contains no card number itself | Yes: linkable payment ID |
| `order_date` | DATE | Recorded calendar date, possibly on the simulation clock | No direct identifier; purchase attribute |

## `order_items` — 408,560 rows

One product line per `(order_id, p_id)` in the operational schema. Quantities
are sales quantities, not current stock. Bronze retains all exported records.

| Column | Source SQL type | Meaning | Personal data? |
|---|---|---|---|
| `order_id` | INTEGER | Parent order | Yes: linkable transaction ID |
| `p_id` | VARCHAR(10) | Purchased product ID | No |
| `qty` | INTEGER | Quantity purchased, positive in the source schema | No direct identifier; purchase attribute |
| `price_at_purchase` | NUMERIC(10,2) | Unit purchase price in EUR | No direct identifier; purchase attribute |

## `review` — 111,322 rows

Product/seller feedback, with source text retained even when empty or NA-like.

| Column | Source SQL type | Meaning | Personal data? |
|---|---|---|---|
| `""` | Export-only index | Legacy CSV row index | No |
| `review_id` | INTEGER | Review identifier; linkable through the review graph | Yes: linkable review ID |
| `buyer_id` | VARCHAR(40), nullable | Review author | Yes: pseudonymous buyer ID |
| `r_desc` | VARCHAR(20000) | Unmodified review body | Possible: free text may contain personal details |
| `title` | VARCHAR(300) | Unmodified review title | Possible: free text may contain personal details |
| `rating` | INTEGER, nullable | Review score from 1 to 5 | No direct identifier; feedback attribute |
| `seller_product_flag` | CHAR(1), nullable | Source seller/product indicator; known legacy inconsistency | No |

## `product_reviews` — 111,322 rows

Bridge between products and reviews; do not join directly to sales without
controlling the resulting row cardinality.

| Column | Source SQL type | Meaning | Personal data? |
|---|---|---|---|
| `""` | Export-only index | Legacy CSV row index | No |
| `p_id` | VARCHAR(10) | Reviewed product | No |
| `review_id` | INTEGER | Linked review, which can identify its author | Yes: linkable review ID |

## `product` — 42,858 rows

Catalogue and current stock, not historical purchased quantities.

| Column | Source SQL type | Meaning | Personal data? |
|---|---|---|---|
| `""` | Export-only index | Legacy CSV row index | No |
| `p_id` | VARCHAR(10) | Product natural key (ASIN) | No |
| `p_name` | VARCHAR(1000) | Product name, including known empty names | No: catalogue text |
| `p_desc` | VARCHAR(5000) | Product-description JSON represented as source text; not parsed here | No: catalogue text |
| `price` | NUMERIC | Catalogue price in EUR; retain source precision | No |
| `qty` | INTEGER | Current available stock | No |
| `category_id` | INTEGER, nullable | Catalogue category reference | No |

## `customer_shipping` — 200,001 rows

Bridge from customers to addresses. More than one address can belong to a
buyer; Gold must resolve a single country mapping before joining sales.

| Column | Source SQL type | Meaning | Personal data? |
|---|---|---|---|
| `""` | Export-only index | Legacy CSV row index | No |
| `address_id` | INTEGER | Address reference | Yes: linkable address ID |
| `c_id` | VARCHAR(40) | Customer identifier, matching an order's `buyer_id` for buyers | Yes: pseudonymous customer ID |
| `is_default` | CHAR(1) | Default-address marker, `0` or `1` | No direct identifier; customer preference |

## `shipping_details` — 200,001 rows

Address attributes with synthetic streets/phones and geographically plausible
postcode/town combinations. Treat each address row as restricted information.

| Column | Source SQL type | Meaning | Personal data? |
|---|---|---|---|
| `""` | Export-only index | Legacy CSV row index | No |
| `address_id` | INTEGER | Address natural key | Yes: linkable address ID |
| `street_address` | VARCHAR(100) | Street and house number | Yes: address |
| `city` | VARCHAR(100) | Town/city | Yes: part of address |
| `state` | VARCHAR(100) | Region | Yes: part of address |
| `zip` | VARCHAR(10) | Postcode; retain leading zeros and letters | Yes: part of address |
| `country` | VARCHAR(60) | Source country value; normalisation belongs in Silver | Yes: part of address |
| `phone` | VARCHAR(15) | National-format mobile number; retain leading zeros | Yes: phone number |

## Excluded sources and secret fields

`customer` and `payment_details` are not selected and have no Step 2 objects.
This keeps `customer.pwd`, `payment_details.card_no` and `payment_details.cvv`
out of Bronze. `customer.fname`, `lname`, `email`, `phone` and
`payment_details.billing_address` / `expiry_date` would also require personal-data
classification if these tables were introduced. Exclude password and card-secret
columns in future connector configuration before any Bronze write; do not rely
on a later SQL model to remove them.

The following Step 3 sections describe implemented/planned schemas, **not new
observed GCS tables**. No live connector or Kafka deployment has been verified.
These fields are not part of the Step 2 CSV snapshot.

## Step 3: custom SQL orders

`extract_orders.py` expects the five operational `orders` fields documented
above, without the CSV export index. Arrow types are `order_id: int64`,
`buyer_id: string`, `discount_id: int64 nullable`, `payment_id: int64 nullable`,
`order_date: date32`. The source SQL result retains nulls and the recorded date.
The extractor does not add `_airbyte_*` fields. Ingestion date is a directory
partition; the state file records the last written object and cursor separately.
Parquet schema metadata also records `source_id`, `source_table` and the batch's
exclusive lower / inclusive upper order-ID watermarks, without changing source columns.
An unexpected source column stops extraction instead of being dropped.

## Step 3: clickstream schema v1

The source producer is [traffic.py](../generators/live/traffic.py). The
[versioned validation schema](../ingestion/clickstream.schema.json) accepts the
three event types below. Types refer to the source JSON; complete source bytes
remain in Parquet `raw_value`. This preserves optional/nested fields even when
they are not projected as separate Parquet columns.

| Common field | JSON type | Meaning | Personal data? |
|---|---|---|---|
| `schema_version` | integer, exactly 1 | Contract version; unknown versions go to DLQ | No |
| `event_id` | UUID string | Producer event identity; useful for later reconciliation | Yes: linkable behaviour ID |
| `event_type` | string enum | `product_viewed`, `added_to_cart` or `order_placed` | No direct identifier; behaviour attribute |
| `event_time` | RFC 3339 string | Simulated business event timestamp; UTC date determines the partition | No direct identifier; behaviour attribute |
| `emitted_at` | RFC 3339 string | Producer's real wall-clock timestamp; preserve separately from event time | No direct identifier; behaviour attribute |
| `buyer_id` | string, 1–40 characters | Links behaviour to a marketplace buyer | Yes: pseudonymous customer identifier |
| `session_id` | UUID string | Links events within a shopping session | Yes: behavioural/session identifier |

| Event-specific field | JSON type / event | Meaning | Personal data? |
|---|---|---|---|
| `p_id` | string / views and cart additions | Product ID | No direct identifier; viewed/purchased item |
| `price` | nonnegative number / views and cart additions | Source product unit price | No direct identifier; behaviour attribute |
| `qty` | positive integer / cart additions | Quantity added | No direct identifier; behaviour attribute |
| `order_id` | positive integer / order placement | Links event to the relational order; never add this sale to relational demand a second time | Yes: linkable transaction ID |
| `payment_method` | string enum / order placement | Credit Card, PayPal or Bank Transfer; no card number | No direct identifier; purchase attribute |
| `amount` | nonnegative number / order placement | Order payment amount in EUR | No direct identifier; purchase attribute |
| `currency` | string, `EUR` / order placement | Payment currency | No |
| `items` | nonempty array of objects / order placement | Purchased order lines | No direct identifier; purchase attributes linked to buyer |
| `items[].p_id` | string | Line product ID | No direct identifier |
| `items[].qty` | positive integer | Line purchased units | No direct identifier |
| `items[].price` | nonnegative number | Unit purchase price supplied by the API | No direct identifier |

The seven common fields are projected into Parquet with original string values
and `schema_version: int32`. Other source JSON fields are in `raw_value`.
Additional Parquet columns are:

| Column | Arrow type | Meaning | Personal data? |
|---|---|---|---|
| `raw_value` | binary | Exact original UTF-8 JSON bytes, including all nested/source fields | Yes: contains buyer/session/purchase data |
| `_kafka_topic` | string | Source topic | No |
| `_kafka_partition` | int32 | Source partition | No |
| `_kafka_offset` | int64 | Offset; together with topic/partition identifies a source record | Indirect: linkable record provenance |
| `_kafka_key` | binary, nullable | Original message key; source generator uses buyer ID | Yes: pseudonymous buyer ID |
| `_kafka_timestamp_ms` | int64, nullable | Broker record timestamp in epoch milliseconds | No direct identifier |
| `_ingested_at` | UTC timestamp string | Consumer processing timestamp | No direct identifier |

Malformed source rows are not silently dropped. Their DLQ JSON fields are
`original_value_base64` (exact payload bytes or null for a tombstone),
`original_key_base64`, `topic`, `partition`, `offset`, `rejection_reason` and
`rejected_at_utc`. The original payload/key may contain personal data and the
DLQ uses the same restricted raw-data access expectations as Bronze.

## Step 3: expected Airbyte metadata

The [GCS 1.0 migration reference](https://docs.airbyte.com/integrations/destinations/gcs-migrations)
documents these field names. They belong to the connector, not the hand-written
extractor. **The installed destination version and actual Parquet physical
representation still need inspection**; do not label this as an observed schema.

| Field | Expected meaning / representation | Personal data? |
|---|---|---|
| `_airbyte_raw_id` | String row identifier assigned during extraction/loading; not a source primary key and not a business-deduplication key | Indirect: linkable ingestion record |
| `_airbyte_extracted_at` | Extraction timestamp; representation depends on destination/output (epoch-ms or logical timestamp must be checked) | No direct identifier |
| `_airbyte_meta` | Structured metadata, including sync/change information; inspect actual nested fields and types | Possible if diagnostic change details include source values |
| `_airbyte_generation_id` | Integer generation identifier; distinguishes refresh generations | No |

Inspect and document any emitted CDC fields such as deletion markers and LSNs
before declaring the connector dictionary complete. The configured catalog
checker validates source fields/modes; it does not manufacture Airbyte metadata
or certify that a destination has produced those fields.

## References

- [Baseline schema](../db/migrations/20260915120000_baseline.sql)
- [Order-item schema](../db/migrations/20260915120100_order_items.sql)
- [Phone-column migration](../db/migrations/20260915140000_widen_phone_columns.sql)
- [Source data and deliberate defects](dataset-card.md)
- [Applied access policy and limitations](../infra/iam.md)
