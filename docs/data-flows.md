# Data flows

## Scope and evidence

Albert's Marketplace serves a European beauty-products shop. Its operational
database stores customers, orders, order lines, reviews and shipping details.
The student platform will isolate analytical reads from customer-serving
systems and publish daily demand by product and country.

Existing source flows are established from code and documentation, not a source
runtime check. F07 is the verified Step 2 cloud upload. Step 3 Python code has
passed local tests; F08–F10 still need live validation on the Docker operator's
machine. Their frequencies remain design goals until that run.

## Flow matrix

| ID | Status | Source | Data | Consumer / destination | Frequency | Format |
|---|---|---|---|---|---|---|
| F01 | Existing source code | Released dataset | Historical database and legacy table extracts | PostgreSQL seed service; student export exercises | Once per initial setup or explicit rebuild | PostgreSQL custom dump; CSV in a compressed archive |
| F02 | Existing source code | Shopper / Streamlit UI | Product lookups, cart changes and checkout requests | FastAPI storefront | On each user action | HTTP requests, query parameters and JSON responses |
| F03 | Existing source code | FastAPI storefront | Product and order reads; customer, cart, order, order-line, payment and shipment writes | Operational PostgreSQL | Synchronously with requests | SQL over the PostgreSQL protocol |
| F04 | Existing source code | Operational PostgreSQL | Buyer IDs, saved-card availability and product catalogue | Live traffic generator | At generator startup; new sign-ups extend its buyer list | SQL results |
| F05 | Existing source code | Live traffic generator | Simulated sign-ups, product lookups, cart changes, checkout and warehouse advancement | FastAPI storefront | Continuous, on an accelerated simulation clock | HTTP and JSON; simulated-time header |
| F06 | Existing source code | Live traffic generator | `product_viewed`, `added_to_cart`, `order_placed` | Kafka topic `clickstream` | Per generated event | UTF-8 JSON, keyed by `buyer_id`, with `schema_version` |
| F07 | Verified: Step 2 | Local marketplace CSV exports for the seven tables listed below; source hashes recorded | Initial raw relational snapshots, all columns retained as strings | `gs://data-engineering-i1-marketplace-group/bronze/snapshot/`, `europe-west1` | Completed 2026-10-26; another run creates a distinct load | CSV input, Snappy Parquet partitioned by `ingestion_date` and `load_id` |
| F08 | CDC setup prepared; deployment pending | Operational PostgreSQL | `orders`, `order_items`, `review`, `product_reviews`, `product`, `customer_shipping`, `shipping_details` | Airbyte CDC, then `bronze/postgres/`; seven-table selection excludes password/card tables | Initial snapshot, then hourly target | Connector records, Parquet/Snappy, ingestion-date/sync partitions |
| F09 | Implemented/local tested; live pending | Operational PostgreSQL `orders` | New visible orders after a saved `order_id`; updates/deletes/late lower IDs missed | psycopg SQL extractor, then `bronze/python/orders/` | Automatic polling every 60 seconds while running | Native SQL values, Parquet/Snappy, ingestion-date partitions |
| F10 | Implemented/local tested; live pending | Kafka `clickstream` | Schema-v1 accepted events; invalid original bytes, reason and time | Python consumer, GCS Bronze and `bronze/_dlq/` | Continuous polls; commit only after output persistence | Raw JSON bytes + metadata in Parquet by UTC event date; DLQ JSON by rejection date |
| F11 | Planned: Step 4 | GCS Bronze | Selected relational tables needed by the demand workload | Native BigQuery working tables | After a successful ingestion batch | Parquet loads into native tables |
| F12 | Planned: Lesson 5 | Selected raw BigQuery tables and Bronze evidence | Typed, reconciled orders and lines; one country mapping per buyer | BigQuery Silver transformations | Daily target | SQL and native tables |
| F13 | Planned: Lesson 5 | BigQuery Silver | Units sold per product, country and order date | BigQuery Gold star, then analyst / business reports | Daily refresh; reports on demand | SQL tables, views and query results |

The Step 3 connector is the authoritative relational ingestion path. The
custom `orders` extractor is a separate learning and comparison path; it must
not be unioned into the connector data used for demand. Initial CSV snapshots
and subsequent connector snapshots also need an explicit handover boundary.

## Risk question 1: Which flows carry personal data?

F01 includes synthetic names, phones, emails, postal addresses, plaintext
passwords and saved-card details. Treat these fields as a governance exercise
even though the dataset card says the customer details are synthetic. F02,
F03 and F05 can carry sign-up details and customer identifiers. F04 reads
buyer identifiers. F06 and F10 contain `buyer_id` and `session_id`, which link
behaviour across events; an `order_placed` event also links to an order.

The seven selected tables in F07/F08 avoid `customer` and `payment_details`,
but still contain buyer/customer IDs, postal addresses, phone numbers and
review free text. F09 carries `buyer_id`. Those identifiers and addresses can
continue into F11/F12 and require restricted access. F13 should expose only
product, country and date dimensions plus aggregate measures.

Keep `customer.pwd` and `payment_details.card_no` / `cvv` outside the selected
export scope. If those tables are added later, exclude these columns in the
connector before any Bronze write and retain evidence of the configuration.
Once a record is admitted into Bronze, retain its values and columns without
cleaning or deduplication. Capture the field inventory in Step 2's dictionary.

## Risk question 2: Which consumers read customer-serving systems?

The storefront (F03), traffic generator (F04), PostgreSQL connector (F08) and
custom extractor (F09) read the operational database. Extraction can compete
with checkout for connections and I/O. Use a read-only ingestion identity,
bounded batch sizes and a measured schedule. The custom cursor path only
captures new, visible IDs: updates, deletes and transactions committing behind
the saved cursor need separate handling and must be reported as limitations.

Business reports should read Gold (F13). Neither reports nor notebook users
should query the operational database for recurring analytical workloads.
These controls are target requirements until credentials and pipelines exist.

## Risk question 3: Where could figures be calculated twice?

- The released history deliberately contains duplicate business orders with
  different order IDs and dates. Summing raw quantities overstates demand;
  uniqueness of `order_id` alone does not solve that defect. Define and test a
  conservative business reconciliation rule in Silver, with an audit trail.
- A purchase appears in both PostgreSQL and an `order_placed` Kafka event.
  PostgreSQL order lines supply units sold. Clickstream supplies behavioural
  analysis; adding its purchases to relational sales counts the same sale twice.
- CSV snapshots, connector batches, the custom extractor and replayed Kafka
  batches can overlap. Keep paths distinct, retain batch/event provenance and
  select the authoritative inputs before curation.
- A buyer can have several addresses. Joining every address to each order
  multiplies its lines. Resolve exactly one default country per buyer; route
  missing or ambiguous mappings to an unknown country and an exception list.
- Joining sales directly to product reviews or shipments can multiply rows.
  Aggregate those subjects separately before combining metrics, and check join
  cardinalities.
- Reports that independently define sales may disagree about duplicates,
  country assignment or event dates. Publish one documented Gold measure.

## Source references

- [Dataset card](dataset-card.md): table inventory, synthetic data and deliberate defects.
- [Database migrations](../db/migrations/): keys and relational structure.
- [Storefront endpoints](../storefront/README.md) and
  [checkout implementation](../storefront/backend/crud.py).
- [Traffic generator](../generators/live/traffic.py): events and simulation clock.
- [Target architecture decision](adr/0001-target-architecture.md) and
  [daily-demand model](model/star-daily-demand.md).
