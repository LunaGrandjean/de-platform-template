# Step 3 ingestion failure modes and credentials

Status: Python jobs implemented and locally tested; PostgreSQL/Kafka/Airbyte
deployment and failure experiments are pending on the Docker operator's machine.
Local tests use fake fetch/commit callbacks and local object storage. They do
not establish live delivery guarantees. See [local evidence](evidence/step3/local-checks.json).

## Failure coverage

| Failure | Handled? | By what | What happens |
|---|---|---|---|
| New PostgreSQL rows | Code: yes for visible higher IDs; live pending | Custom `order_id` cursor; proposed connector CDC | Extractor writes a bounded ID window then checkpoints each durable batch. A lower ID committing late can still be missed. |
| Updates | Custom extractor: **no**; connector: planned | ID cursor does not revisit old rows; proposed CDC WAL stream | Old extractor values remain. Connector behaviour must be verified with a real update. |
| Deletes | Custom extractor: **no**; connector: planned | No deletion query; CDC is the proposed alternative | Bronze retains original records. A connector tombstone/change record is needed for later reconciliation. |
| Source outage | Initial connection: yes, bounded; mid-query: stops | Five psycopg attempts with 1/2/4/8-second backoff; read timeout | After exhaustion or a query failure, job exits with its last completed checkpoint. Restart after recovery. No infinite retry promise. |
| Malformed Kafka row | Code: yes; broker test pending | UTF-8/JSON parsing, JSON Schema and DLQ | Invalid original bytes, source offset, reason and rejection time are durably written before committing. Valid events continue. |
| SQL malformed row / schema drift | Fail closed | Explicit expected columns, Arrow schema and key checks | Stops instead of silently dropping/casting away fields; no checkpoint for the failed batch. |
| Event schema change | Code: yes by rejection | `schema_version=1`, typed schema, forbidden extra fields | Unknown versions/fields go to DLQ. Update schema and tests deliberately before accepting a new version. |
| Duplicate after restart | Expected, retained | Upload precedes cursor/offset commit | A failed checkpoint/commit can replay already landed data. Local simulations measured two replayed rows in each tested path. Reconcile later using source keys/offset provenance. |
| Expired credential | Stops safely; renewal not automated | ADC refresh where supported, driver/client errors | Storage errors prevent cursor/offset advancement. An operator must restore authorised access. No secrets are logged or copied into Git. |
| DLQ/storage write failure | Code: yes | Commit occurs only after every output write succeeds | The whole Kafka poll remains uncommitted, even if some valid files were already written; replay may duplicate them. |
| Kafka rebalance / failed offset commit | At-least-once replay within retention | Synchronous explicit next-offset commits | Objects may already exist when commit fails. Restart/reassignment can produce duplicates; no exactly-once claim. |
| Kafka retention expires before processing | **No recovery of removed events** | Consumer starts/resets at earliest available offset | Events no longer retained by Kafka cannot be recovered from that broker. Monitor lag; increase retention or restore from another source. |
| PostgreSQL out-of-order transaction commits | Custom extractor: **not handled** | `order_id` is not a commit timestamp | A low ID becoming visible after the saved cursor can be missed. The local simulation demonstrates one missed lower-ID row. |
| WAL limit invalidates CDC slot | Pending live validation; not automatically repaired | Source compose requests `max_slot_wal_keep_size=512MB` | A lost slot requires operator intervention and resnapshotting. Verify live values and retained bytes; do not promise that PostgreSQL will automatically delete the slot. |
| Source rebuild / reset | Partial detection | `SOURCE_ID` binding and backwards maximum check | A detectable backwards watermark fails. Other resets can be missed; deliberately choose a new source identity/state and define handover. |
| Concurrent custom extractors | Same state file: rejected | OS file lock | A second writer cannot acquire that local checkpoint. Separate machines/state files are not coordinated; use one operator. |
| Source `TRUNCATE` | **Not handled by the proposed publication** | Publication includes INSERT/UPDATE/DELETE only | Coordinate source resets explicitly; do not treat a truncate as ordinary row deletes. |

## Credential locations, access and revocation

| Credential | Location / who may access it | Revocation / current evidence |
|---|---|---|
| PostgreSQL ingestion password | Operator's ignored `.env` / environment; approved operators only. Source role has SELECT on the seven tables and CDC replication privilege. | Source owner changes the password or disables LOGIN. SQL is provided, but this role/password has not been deployed here. Never commit a connection URL containing a password. |
| Python GCS identity | Existing local ADC profile of the person running the jobs; not copied into the repository or shared between members | Revoke the relevant user grant/session through the course account process. No Step 3 service-account/HMAC key is created by these Python jobs. |
| Airbyte GCS HMAC key | Proposed dedicated service-account HMAC credential in Airbyte's secret configuration; project administrator / designated connector operator | Administrator can mark the key inactive and delete it. Creation, actual owner/access and any cleanup are pending; do not invent a key ID or claim revocation. |
| Kafka TLS/SASL credentials, if configured | Operator's ignored environment and authorised certificate paths | Broker administrator rotates/revokes them. Provided source compose is local plaintext, so no SASL secret is claimed as configured. Use approved local/tunnel access. |

Repository `.gitignore` excludes `.env`, key/credential JSON patterns, generated
data and cursor files under `data/`. This is an aid, not a substitute for reviewing
exports/screenshots before commit. Never put passwords in shell arguments or
paste complete Airbyte exports into a PR. The catalog checker emits only a
safe schema/selection summary.

Step 2's [actual bucket IAM](../infra/iam.md) remains the verified cloud-access
baseline. No additional Step 3 IAM changes, keys, connector jobs or replication
slots are claimed as deployed. The operator must inventory and remove only
confirmed unused group slots/keys; never drop another group's resources.

## Operational actions

Use [the Docker handoff](runbooks/step3-docker-handoff.md) for live setup,
malformed-event verification, source-failure measurements and slot/key cleanup.
Keep connector, custom and snapshot paths separate. Choose an explicit
snapshot-to-connector handover before using them together in a warehouse.
