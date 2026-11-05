# ADR 0003: Python orders extraction and Kafka ingestion (Route 3)

- Date: 2026-11-05
- Status: Route selected; Debezium CDC, GCS writes, connector/sink restart-resume and synthetic CRUD verified live; Step 2/3 inventories and Parquet footer checks recorded
- Author: Group

## Context

The operational source is PostgreSQL and the behavioural source is the Kafka
`clickstream` topic. The course route selected by the group is Route 3, which
uses Python ingestion without Airbyte. Step 2's raw snapshot already exists in
GCS. Step 3 must keep subsequent ingestion paths distinguishable from that
snapshot and preserve replay evidence.

PostgreSQL's mutable tables do not all have a reliable update timestamp. The
implemented Python exercise extracts only `orders` by increasing `order_id`;
this is a bounded incremental scan, not a complete change-data-capture stream.
Kafka supplies clickstream events with source offsets and a versioned JSON
envelope.

## Decision

| Source / purpose | Selected mode | Guarantee and limitation |
|---|---|---|
| PostgreSQL `orders` exercise | Python/psycopg, parameterised read-only SQL, persistent `order_id` cursor, append-only Parquet | Upload precedes cursor update. A crash can replay. Updates, deletes and lower IDs committing after the watermark are missed. This path is not authoritative CDC. |
| Kafka clickstream | Python consumer, JSON Schema v1, Parquet for valid messages, JSON DLQ for invalid messages, synchronous manual offset commit after all writes | At-least-once within Kafka retention. A crash between write and commit can duplicate objects; expired source events cannot be recovered. |
| PostgreSQL WAL changes | Debezium Connect reads the existing `marketplace_bronze_slot` and `marketplace_bronze_publication`; the Python consumer appends raw events to `bronze/cdc/` | Snapshot baseline overlaps prior data; at-least-once delivery reconciles with topic/partition/offset; treat TRUNCATE as a source reset requiring a coordinated full snapshot and downstream reconciliation. |

Airbyte is not part of the selected implementation. The existing files under
`ingestion/airbyte/` are unapplied proposal artifacts and do not define a live
connector, destination, or data path.

## Bronze paths

- Orders: `bronze/python/orders/ingestion_date=.../<uuid>.parquet`.
- Accepted Kafka events: `bronze/kafka/<topic>/event_date=<UTC date>/<uuid>.parquet`.
- Rejected Kafka events: `bronze/_dlq/<topic>/rejection_date=<UTC date>/<uuid>.json`.
- Step 2 CSV snapshot: `bronze/snapshot/...`; it remains a separate initial load.

No job silently deduplicates business records. Kafka provenance records
topic/partition/offset, and the complete event payload remains in `raw_value`.
Invalid events retain the original bytes/key in base64 with rejection context.
These payloads can contain linkable buyer and session information, so the DLQ
requires the same restricted access as raw Bronze.

## Evidence and operational status

The local fault-injection evidence records 25 offline checks using real local
Parquet/checkpoint writes and simulated SQL/Kafka callbacks. It does not prove
live service behaviour. `docs/evidence/step3/route3-live-run.json` records an
operator-reported orders batch and an isolated Kafka valid/invalid/valid test.
The [read-only Bronze audit](../evidence/step3/bronze-verification.json)
verified object listings and Parquet footers: after catch-up there are 50
orders files / 225,381 rows, 129 main clickstream files / 29,144 rows, two
isolated test-topic rows, and one DLQ object. The reported 13-row batch and the
237-row catch-up footer both match the recorded checkpoints. No row values or
DLQ body were read. Two source orders were already beyond the new watermark at
the post-run check.

The PostgreSQL source uses `wal_level=logical`, has the seven-table publication
and the group's existing `pgoutput` slot. Debezium 3.7.0.Final completed an
initial snapshot and is streaming changes through that slot; the Python sink
was observed writing all seven table topics to GCS. At 16:18 UTC the sink had
zero lag on 21 partitions and the slot retained 144,920 WAL bytes after the
heartbeat topic allowed the confirmed LSN to advance;
see [`cdc-status.json`](../evidence/step3/cdc-status.json).

## Failure and delivery consequences

- The extractor retries initial connection failures up to five times with
  1/2/4/8 second waits, then stops. A failed batch does not advance the cursor.
- The cursor is local state guarded against concurrent writers to the same
  file. Separate machines are not coordinated.
- A crash after a durable object upload but before state/offset commit can
  leave duplicate Bronze objects. Downstream reconciliation uses the cursor
  bounds or Kafka topic/partition/offset provenance.
- Kafka invalid messages are written to the DLQ before offset commit. A DLQ
  write failure prevents that poll from committing.
- The CDC sink resumed on the same group after restart; a synthetic product
  row produced create/update/delete events plus a tombstone, then was removed.
  This is a focused demonstration, not broad fault-injection coverage. Do not
  drop or recreate the group's slot during routine operation.

## References and review triggers

- [Python ingestion guide](../../ingestion/README.md)
- [Failure modes and credentials](../ingestion-failure-modes.md)
- [Live run evidence](../evidence/step3/route3-live-run.json)
- [Local fault-injection evidence](../evidence/step3/local-checks.json)
- [Kafka consumer manual offsets](https://kafka-python.readthedocs.io/en/3.0.0/apidoc/KafkaConsumer.html)
- [Psycopg connections](https://www.psycopg.org/psycopg3/docs/api/connections.html)

Revisit this ADR if the course requires full CDC, if the source schema gains a
reliable change cursor, or if the observed volume makes the current file sizing
or polling strategy unsuitable. Any new route must preserve Bronze paths and
make its delivery guarantee and source handover explicit.
