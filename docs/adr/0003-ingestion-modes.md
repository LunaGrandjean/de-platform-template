# ADR 0003: CDC connector, insert cursor and validated Kafka batches

- Date: 2026-10-26
- Status: Python implementation locally verified; connector/live deployment pending
- Author: Luna Grandjean
- Constraint: Docker source services will run on a group member's machine

## Context and decision

The source consists of mutable PostgreSQL tables and a versioned JSON clickstream.
Most relational tables have no `updated_at` column. An integer order ID can
identify higher new IDs, but it does not describe updates, deletes or commit
order. The group needs three ingestion methods with explicit limits while
preserving raw arrivals in the existing GCS bucket.

| Source / purpose | Chosen mode | Rejected option and reason | Guarantee / limitation |
|---|---|---|---|
| Seven required PostgreSQL tables, authoritative relational path | Proposed Airbyte CDC via pgoutput, Incremental / Append | A single business-date/ID cursor cannot capture all updates/deletes; Full Refresh / Overwrite would remove arrival evidence | Initial snapshot plus WAL changes, conditional on valid slot/state; actual behaviour must be demonstrated. No deployed guarantee yet. |
| Hand-written `orders` exercise | psycopg, parameterised SQL, persistent `order_id` cursor | ORM/framework would hide the required SQL; using `order_date` would miss same-date inserts and complicate a reliable tie-breaker | Batch written before atomic local checkpoint. Crash can duplicate; updates, deletes and late lower-ID commits are missed. Separate from authoritative sales. |
| Kafka clickstream | Poll batches, JSON Schema v1, Parquet plus JSON DLQ, manual synchronous offset commits | Auto-commit can acknowledge before storage; storing only accepted events would silently discard malformed arrivals; an exactly-once claim is not justified | At-least-once conditional on Kafka retention, storage and stable source identity. Replay duplicates remain in Bronze. |

The connector excludes `customer` and `payment_details` entirely. Its publication,
read grants and selected-stream allowlist contain only the seven required safe
tables. This is the current planned protection for `pwd`, `card_no` and `cvv`;
the operator must commit actual field-selection evidence before claiming it is
applied. Python's extractor never queries those tables.

## Storage and partition choices

- Connector: `bronze/postgres/<stream>/ingestion_date=.../sync_id=.../`, proposed
  Parquet/Snappy and append mode. Source changes and connector metadata coexist;
  consumers must reconcile them later.
- Custom extractor: `bronze/python/orders/ingestion_date=.../<uuid>.parquet`.
  Five source SQL columns use native Arrow types, including nullable values and
  `DATE`. A schema mismatch stops the job rather than dropping columns. Unlike
  the CSV snapshot, there is no unnamed export index in SQL results.
- Kafka: `bronze/kafka/<topic>/event_date=.../<uuid>.parquet`. Choose UTC
  `event_time` instead of `emitted_at`: the first tracks the simulated business
  event, while the second tracks real producer wall time. Keep both strings and
  the complete original bytes; late events may write to older partitions.
- DLQ: `bronze/_dlq/<topic>/rejection_date=.../<uuid>.json`, with base64 original
  value/key, source offsets, reason and rejection timestamp. Invalid JSON has
  no trustworthy business timestamp, so rejection date is the partition key.

The accepted-event envelope exposes common fields and transport provenance.
Event-specific/nested fields are retained in `raw_value`, not silently omitted
or flattened. All source bytes remain available for replay. No deduplication
occurs in any of these Bronze jobs.

## Measured local evidence and missing Exercise 1 results

The [local-check record](../evidence/step3/local-checks.json) records 25 successful
offline tests using real local Parquet files but simulated SQL fetches and Kafka
commits. These are fault-injection observations, not live Exercise 1 results:

| Local scenario | Observed result |
|---|---|
| Two SQL rows uploaded, checkpoint intentionally fails, then restart | Two duplicate rows retained across the two output batches |
| Two valid Kafka events plus one invalid event; offset commit fails, then replay | Two duplicate valid rows; duplicate rejection evidence can also exist |
| Cursor advanced to ID 3 before ID 2 becomes visible | One lower-ID row missed; an update to an old row also remains unseen |
| Valid → malformed → valid in one local batch | Two accepted records and one durable DLQ object; next offset 3 committed by the fake callback |
| Connection fails five times | Five attempts, waits of 1/2/4/8 seconds, then stop |
| DLQ/storage failure | No Kafka offset commit for the failed poll |

**Pending real measurements:** source/connector duplicate counts after restart,
actual missed cursor changes and replication-slot retained WAL before/after
pausing CDC. No WAL bytes, Airbyte job ID or live malformed-event success is
invented. Add the group's own Exercise 1 measurements after the operator runs
[the live procedure](../runbooks/step3-docker-handoff.md).

## Operational and security consequences

The custom cursor is local durable state: one operator owns it and an OS lock
rejects competing writers on that file. A change of source requires a deliberate
state/handover decision. The extractor bounds initial retry attempts and stops
on unrecovered query/storage failures; it does not promise unattended recovery
from every failure or provide a scheduler/supervisor.

Kafka commits only after all valid and invalid outputs from a poll are stored.
Writes and commits are not one transaction, so duplicates are an expected
trade-off. Long storage pauses can cause rebalances; replay remains possible.
Earliest-offset reset cannot recover events already expired from Kafka.

The Docker operator also owns CDC slot monitoring. The source compose requests
a 512 MB slot-WAL limit, but the runtime value and observed retained bytes must
be checked. An invalidated slot requires investigation/resnapshotting; unused
test slots must be removed after their readers stop. Python uses each operator's
own ADC; Airbyte's documented GCS destination uses a separately administered
HMAC key. No Step 3 key creation/revocation or least-privilege deployment is
asserted in this ADR. See [credential and failure details](../ingestion-failure-modes.md).

## References and review triggers

- [PostgreSQL connector](https://docs.airbyte.com/integrations/sources/postgres)
- [GCS connector](https://docs.airbyte.com/integrations/destinations/gcs)
- [Kafka manual offsets](https://kafka-python.readthedocs.io/en/3.0.0/apidoc/KafkaConsumer.html)
- [Psycopg connection behaviour](https://www.psycopg.org/psycopg3/docs/api/connections.html)

Reconsider the ID cursor if updates/deletes become required on its learning path;
use the connector's CDC path rather than implying that a higher-ID scan solves
them. Revisit buffering when files become persistently tiny or writes approach
the consumer's poll interval. Review deployed versions/credentials and the
guarantee after the first complete live run, then update architecture status.
