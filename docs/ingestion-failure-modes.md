# Step 3 ingestion failure modes and credentials

Status: Route 3 (Python, no Airbyte) is selected. The orders extractor and
clickstream consumer have local fault-injection checks. Debezium snapshot,
CDC GCS writes, restart/resume and one synthetic CRUD sequence were verified
live on 2026-11-05; this does not establish exact-once delivery. See [local evidence](evidence/step3/local-checks.json),
[run evidence](evidence/step3/route3-live-run.json) and
[Bronze verification](evidence/step3/bronze-verification.json).

## Failure coverage

| Failure | Handling and observed limit |
|---|---|
| New PostgreSQL orders | The custom `order_id` cursor extracts higher visible IDs and checkpoints after each durable batch. It can miss lower IDs that commit late. GCS has 225,381 rows through watermark 225405; the post-run source check found 2 further rows. |
| PostgreSQL updates/deletes | The orders cursor does not revisit old rows or read WAL. Debezium CDC captures row-level changes for the seven publication tables; publication excludes TRUNCATE. |
| Source outage | Initial connection retries are bounded to five attempts with 1/2/4/8-second backoff. Unrecovered connection/query/storage errors stop the job and retain the last completed checkpoint. |
| SQL schema drift | The extractor checks expected columns and Arrow schema and fails closed instead of silently dropping fields. A failed batch does not advance the cursor. |
| Cursor replay | Upload-before-checkpoint can duplicate rows after a crash. Local fault injection observed two replayed rows in its scenario. Downstream jobs must reconcile with source keys and batch watermarks. |
| Concurrent extraction | An OS lock prevents two writers to the same local state file. Separate machines are not coordinated; use one checkpoint owner. |
| Malformed Kafka message | Invalid UTF-8/JSON, tombstones, duplicate keys and schema violations are written to the DLQ with original bytes/key, source offset, reason and rejection time. The recorded live test used an isolated topic; verify GCS objects independently. |
| DLQ/storage write failure | The consumer commits only after all valid and rejected outputs for a poll are stored. A failed write prevents that poll from committing. |
| Kafka commit failure or crash after write | At-least-once replay can create duplicate Parquet/DLQ objects because writes and offset commits are not transactional. Local fault injection demonstrated replay. |
| Kafka retention expires | Events already removed from Kafka cannot be recovered by this consumer. Monitor lag and retention. |
| Event schema change | The consumer enforces `schema_version=1` and rejects unknown fields/versions. Update the schema deliberately before accepting a new version. |
| PostgreSQL source reset or `TRUNCATE` | The cursor only detects some backwards-watermark resets. `TRUNCATE` and other rebuilds require an explicit source identity/checkpoint handover. |
| Logical slot lag | Debezium uses `marketplace_bronze_slot`; after provisioning the heartbeat topic, `confirmed_flush_lsn` advanced. Retained WAL was 144,920 bytes at 16:18 UTC. The configured 512 MB limit can invalidate a lagging slot; keep monitoring connector state and WAL. See [CDC status evidence](evidence/step3/cdc-status.json). |

## Credentials and access

| Credential/access | Location and status | Handling |
|---|---|---|
| PostgreSQL ingestion login | Ignored local `.env`; Debezium uses `marketplace_ingest` with `REPLICATION`, and the Python extractor uses the same login for SQL. | Keep credentials local. Connector config is registered over local Connect REST. Rotate/disable through the source owner. Never put a connection URL or password in Git/logs. |
| Python GCS identity | The current ADC successfully listed Bronze objects and read Parquet footers; write permission was not tested in this audit. | Each operator authenticates as themselves. Do not copy ADC files or share key material. Storage errors must not advance cursor/offset state. |
| Kafka access | Current source compose uses local plaintext listeners. | Use only on the operator's host or approved tunnel. If TLS/SASL is introduced, keep credentials/certificates in ignored local configuration and rotate through the broker owner. |
| Airbyte GCS HMAC key | Not applicable to the selected route; no Airbyte destination or key is claimed. | Do not create one for Route 3. |

The repository `.gitignore` excludes `.env`, credential JSON patterns, generated
data and cursor files under `data/`. Review any evidence before commit; do not
include raw source rows, raw event bodies, ADC files, passwords or keys.

Step 2's [bucket IAM record](../infra/iam.md) is the verified cloud-access
baseline. No Step 3 IAM change is claimed. The observed inactive replication
slot should be inventoried with its owner before any cleanup; never drop a
slot solely because it is inactive.

## Operational references

- [Route 3 ingestion guide](../ingestion/README.md)
- [Route 3 runbook](runbooks/step3-docker-handoff.md)
- [Ingestion ADR](adr/0003-ingestion-modes.md)

Keep the snapshot, orders cursor output and Kafka output in separate paths.
Before warehouse use, define which relational path is authoritative and how
snapshot overlap and replay duplicates will be reconciled.
