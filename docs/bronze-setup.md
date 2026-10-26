# Step 2: Bronze setup and verification

## Deployed result

- Project: `data-engineering-i1`.
- Shared group bucket: `gs://data-engineering-i1-marketplace-group`.
- Region: `europe-west1`; default storage class: Standard.
- Initial load: `20261026T105159Z-2efcb86a`, ingestion date `2026-10-26`.
- 7 raw tables, 1,296,708 retained rows, 19 Snappy Parquet objects.
- Completion manifest: `bronze/_manifests/20261026T105159Z-2efcb86a.json`.

Use [Cloud Console](https://console.cloud.google.com/storage/browser/data-engineering-i1-marketplace-group?project=data-engineering-i1)
to inspect the bucket with your authorised Google identity. No bucket URL grants
access by itself.

## Use uv without an administrator install

The root `pyproject.toml` and `uv.lock` define the platform tools. They are
separate from the source generator's project in `generators/`. Use Python 3.12
and `uv` from this repository root. The local `.venv`, cache, CSVs and Parquet
outputs remain ignored by Git.

```powershell
$env:UV_CACHE_DIR = Join-Path (Get-Location) 'data/uv-cache'
uv sync --locked
uv run --locked -m unittest discover -s tests -p test_bronze_snapshot.py -v
```

Cloud operations use existing Application Default Credentials (ADC). They do
not require `gcloud`, `bq` or Windows administrator rights. If ADC is missing or
expired, use the course-approved Google sign-in process to restore local
credentials; the repository does not bootstrap authentication or create a
service-account key. Do not copy a credential file into the project or commit it.

## Reproduce a new local snapshot

Place the seven released CSV files in an input directory. On Luna's checkout
they are available in the sibling exercise directory; the script only reads
those inputs and does not modify that repository.

```powershell
uv run --locked scripts/prepare_bronze.py --source-dir '../data-engineering/legacy_csv'
```

The script creates a new unique load, verifies all parsed field values against
a Parquet re-read, and prints the local manifest path. It refuses malformed
field counts and existing output directories. Failed runs leave no completion
manifest, so their partial files must not be consumed.

To upload a new verified snapshot, substitute the **exact manifest path printed
by that run**:

```powershell
uv run --locked scripts/gcs_bronze.py upload --project data-engineering-i1 --bucket data-engineering-i1-marketplace-group --manifest 'data/bronze/manifest-<load-id>.json'
```

Reusing the same manifest is safe: matching objects are verified and reused;
different objects are never overwritten. A different load intentionally adds
another snapshot. Readers must choose a completed load instead of unioning all
snapshots. The actual upload receipt is stored in
`docs/evidence/step2/upload-receipt.json`.

## Inspect live settings

```powershell
uv run --locked scripts/gcs_bronze.py inspect --project data-engineering-i1 --bucket data-engineering-i1-marketplace-group
```

This reads live bucket metadata and bucket IAM, saves their API responses, and
exports the **observed** lifecycle rules to `infra/lifecycle.json`. It makes no
cloud configuration changes. It checks the bucket region; inspect the exported
policy when auditing the other controls.

The one-time creation command used for Step 2 was:

```powershell
uv run --locked scripts/gcs_bronze.py create --project data-engineering-i1 --bucket data-engineering-i1-marketplace-group
```

Do not rerun creation for an existing bucket: it deliberately fails rather than
replacing or reconfiguring a bucket that may hold data. The creation code
applies public access prevention, uniform access and non-deleting lifecycle
rules, then reads the resulting settings back.

## Verification and remaining group actions

- [x] Create and inspect the bucket in the required region.
- [x] Retain all columns and parsed CSV values in partitioned Parquet.
- [x] Verify local row-value hashes and remote object checksums.
- [x] Publish a completion manifest only after the seven tables are uploaded.
- [x] Record the layout ADR, full dictionary, costs, actual IAM and lifecycle.
- [x] Test duplicate/text preservation, malformed records, overwrite refusal
  and an empty table's schema (four tests passed).
- [ ] Have all members verify access using their own Google identities;
  existing project grants have not been audited for group membership.
- [ ] Have every member commit and merge at least one reviewed group PR.

The current bucket uses broad project convenience-group bindings. Its actual
scope and the limits of the access audit are explained in [infra/iam.md](../infra/iam.md).
