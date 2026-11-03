# Step 3: Docker operator handoff

## Scope and current status

Luna's laptop cannot enable Docker's Windows virtualisation prerequisite without
an administrator. A group member with working Docker takes this procedure.
The source stack and live tests below have **not** been run as part of this
Step 3 preparation. Do not mark the step complete until the evidence checklist
at the end is filled with actual results.

## 1. Get the group branch and start the source

Obtain the latest `Luna` work from the group's repository after it is committed
and pushed. From the repository root on the Docker operator's machine:

```powershell
docker info
docker compose up -d
docker compose ps
docker compose logs --tail 30 seed migrate kafka-init
```

The existing compose file already provides PostgreSQL, Kafka, the storefront
and the traffic generator. Airbyte is a separate installation: use the
course-approved Airbyte setup and record its platform/connector versions.
Do not invent an Airbyte compose file or assume its container names/network.
Verify that the database contains the seeded seven tables and that `clickstream`
receives events before configuring ingestion.

Never use `docker compose down -v` for a normal restart: it deletes the source
volumes and invalidates saved extraction state. The group uses one source copy;
independent reseeded laptops are different sources even if their IDs overlap.

## 2. Prepare the ingestion identity and CDC

Run the committed SQL against this source database:

```powershell
Get-Content ingestion/airbyte/setup_postgres.sql -Raw | docker compose exec -T db psql -U postgres -d marketplace
docker compose exec db psql -U postgres -d marketplace
```

At the interactive psql prompt, set the role password with
`\password marketplace_ingest`; do not paste a real password into a committed
SQL file or shell command. Verify its role attributes and grants. If reusing an
existing role, inspect inherited grants; the script does not revoke unknown
existing privileges. CDC requires its dedicated role to have `REPLICATION`.
Do not grant SELECT on `customer` or `payment_details`.

Check `SHOW wal_level;` and `SHOW max_slot_wal_keep_size;`. The provided compose
requests `logical` and `512MB`; those values are source-code settings, not a
substitute for recording live results. Inspect the publication and confirm it
contains exactly the seven required public tables.

Only when ready to start Airbyte, create its dedicated slot using pgoutput:

```sql
SELECT * FROM pg_create_logical_replication_slot('marketplace_bronze_slot', 'pgoutput');
```

Do not recreate an existing slot blindly. Inspect
`ingestion/airbyte/inspect_slots.sql` first and verify ownership. A slot holding
WAL while its reader is stopped can consume disk; the 512 MB limit can invalidate
the slot and require resnapshotting. It does not guarantee indefinite retention.

## 3. Configure and test Airbyte

Use [the setup contract](../../ingestion/airbyte/connection-plan.json) as a
checklist. It is **not** an importable/exported runtime configuration.

1. Create a PostgreSQL source with the dedicated role. Select CDC and enter the
   actual publication/slot names. Use a host reachable from the Airbyte worker:
   `localhost` inside a container is that container, not the PostgreSQL service.
2. Discover streams. Select exactly `orders`, `order_items`, `review`,
   `product_reviews`, `product`, `customer_shipping`, `shipping_details` in `public`.
   Keep all their source fields selected. Leave every other table unselected,
   particularly `customer` and `payment_details`. This excludes `pwd`, `card_no`
   and `cvv` before any Bronze write. Disable automatic selection of new tables.
3. Set the GCS destination to the existing group bucket, region `europe-west1`,
   path `bronze/postgres`, Parquet and Snappy. The documented GCS destination
   v1.0.0 uses HMAC credentials from a service account, not the Python jobs' ADC.
   Obtain approved credentials from the project administrator; record the owner
   and revocation procedure. Do not create keys merely to bypass denied IAM.
4. Configure the path format shown in the plan. Use Incremental / Append and
   an hourly schedule. Do not use Full Refresh / Overwrite: it can delete prior
   Bronze objects. The connection test needs to create/list/delete a probe
   object; have the administrator grant the required bucket permissions.
5. Run the initial sync and a second incremental sync. Record actual job IDs,
   stream counts, GCS paths and the metadata fields in a landed Parquet file.

Current official references, checked 2026-10-26:
[PostgreSQL source](https://docs.airbyte.com/integrations/sources/postgres) and
[GCS destination](https://docs.airbyte.com/integrations/destinations/gcs).
Version-specific UI field names can differ; export the actual installed
configuration rather than treating the plan as deployed proof.

Keep raw exports under ignored `data/`; they can contain credentials. Run:

```powershell
$env:UV_CACHE_DIR = Join-Path (Get-Location) 'data/uv-cache'
uv sync --locked
uv run --locked ingestion/validate_connector_export.py data/airbyte-configured-catalog.json
```

Commit only the generated field-selection summary and manually reviewed,
redacted screenshots/settings. The checker does not certify CDC or a live sync.

## 4. Start the Python jobs

Use the [ingestion instructions](../../ingestion/README.md), fill an ignored
local `.env`, and authenticate to GCP as your own authorised course identity.
Do not copy Luna's ADC file. Run the SQL extractor and Kafka consumer in separate
terminals; keep their state/group identities stable across restarts.

The simplest arrangement runs both jobs on the Docker operator's machine.
If Luna runs them remotely, arrange approved connectivity first. The existing
Kafka HOST listener advertises `localhost:9092`; changing only the bootstrap
hostname will not make this broker remotely reachable. Configure a suitable
tunnel/listener with the operator. Never solve this by exposing the source's
unauthenticated default ports to the public internet.

## 5. Prove malformed-event handling on a real broker

Use an isolated topic, group ID and the resulting separate Bronze topic prefix.
Create the topic explicitly, particularly if auto topic creation is disabled:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic clickstream-step3-check --partitions 1 --replication-factor 1
uv run --locked ingestion/publish_test_events.py
uv run --locked ingestion/consume_clickstream.py --topic clickstream-step3-check --group-id marketplace-step3-check --idle-seconds 15
```

The publisher sends valid, malformed and valid events with one key and prints
their actual offsets. Expect two accepted records and one rejection across the
consumer's poll summaries. Verify the matching topic/partition/offset in:

- `bronze/kafka/clickstream-step3-check/event_date=.../*.parquet`;
- `bronze/_dlq/clickstream-step3-check/rejection_date=.../*.json`.

Confirm the second valid event landed after the rejection and record its offset.
Restart the **same** group without publishing new events: no already committed
records should be reprocessed. To rerun the full scenario, publish new messages
or deliberately choose a new test group; do not reset the production group.
Test-topic records remain isolated from the business clickstream.

## 6. Capture failures, measurements and key/slot cleanup

Measure from your real Exercise 1 / running source: duplicate counts after a
forced restart, changes missed by the ID cursor, and retained WAL bytes before
and after stopping/resuming the connector. Record conditions and timestamps.
Do not substitute the committed local simulations for these live measurements.
Exercise INSERT, UPDATE and DELETE on approved synthetic test rows and compare
the source, connector and custom-extractor results. Do not delete existing
business records just to obtain evidence.

Inventory slots with `ingestion/airbyte/inspect_slots.sql`. Stop the associated
reader, confirm slot ownership/inactivity, then drop only unused group test
slots. Keep the active CDC slot. Record any HMAC/service-account key created for
the connector and who can revoke it. Mark unused HMAC keys inactive and delete
them through the project administrator; do not revoke the live connector's key
while it is still needed. The Python jobs themselves create no such keys.

## Evidence to return in the group branch

- [ ] Platform/source/destination connector versions and redacted settings.
- [ ] Real catalog summary proving all seven streams and secret-table exclusion.
- [ ] Two successful incremental/CDC jobs, actual GCS object paths and metadata.
- [ ] SQL cursor persisted across restart, with measured duplicate/missed cases.
- [ ] Real malformed-event publish offsets, matching DLQ object and later valid event.
- [ ] Actual WAL-retention measurements and confirmed unused-slot cleanup.
- [ ] Credential owner, granted scope and key revocation/unused-key cleanup record.

Place safe summaries in `docs/evidence/step3/`, update the ADR/failure table and
architecture status, then commit your work for group review. Never commit raw
event bodies, personal source rows, passwords, HMAC secrets or key JSON files.
