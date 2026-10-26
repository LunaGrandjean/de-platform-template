# Actual Bronze bucket IAM

Verified on 2026-10-26 using the Google Cloud Storage Python SDK, run with
`uv`. Bucket: `gs://data-engineering-i1-marketplace-group`; project:
`data-engineering-i1`; region: `europe-west1`.

The committed [bucket policy](../docs/evidence/step2/bucket-iam.json),
[bucket metadata](../docs/evidence/step2/bucket.json) and
[verification record](../docs/evidence/step2/verification.json) are actual API
responses. This table expands every member in the returned bucket bindings.

| Principal | Actual role | Scope | Justification and limitation |
|---|---|---|---|
| `projectOwner:data-engineering-i1` | `roles/storage.legacyBucketOwner` | This bucket | Default project-owner administration binding. Retained for course-project administration; broader than daily ingestion needs. |
| `projectEditor:data-engineering-i1` | `roles/storage.legacyBucketOwner` | This bucket | Default project-editor administration binding. Retained during initial setup; broader than daily ingestion needs. |
| `projectViewer:data-engineering-i1` | `roles/storage.legacyBucketReader` | This bucket | Default project-viewer bucket-metadata access. Actual viewer-group membership has not been enumerated. |
| `projectOwner:data-engineering-i1` | `roles/storage.legacyObjectOwner` | Objects in this bucket | Default project-owner object control. Necessary for current administration, but not a least-privilege pipeline role. |
| `projectEditor:data-engineering-i1` | `roles/storage.legacyObjectOwner` | Objects in this bucket | Default project-editor object control, used for the initial course workflow. Broader than create-only ingestion. |
| `projectViewer:data-engineering-i1` | `roles/storage.legacyObjectReader` | Objects in this bucket | Default read access for the project's viewer convenience group. Not restricted to the four named GitHub team members. |

These convenience-group bindings were returned after bucket creation. Their
presence is documented rather than represented as a custom least-privilege
policy. No `allUsers` or `allAuthenticatedUsers` member appears in this bucket
policy. Public access prevention is `enforced`; uniform bucket-level access is
enabled, so object ACLs are disabled. The bucket also has the observed default
seven-day soft-delete policy. No retention lock was applied.

## Access boundaries still to verify

Bucket IAM responses do **not** enumerate all inherited project, folder or
organisation permissions. Effective access can be broader. Project-group
membership was not audited here, and access for Omar, Malek and Anas has not
been individually tested. GitHub handles are not Google IAM principals. Ask
members to test read access using their own course identity; if a grant is
needed, obtain their actual Google principal from the project administrator.

Do not claim that only this group can read Bronze. Raw addresses, phone numbers,
buyer IDs and review text are classified in the
[data dictionary](../docs/data-dictionary.md). Before production use, replace
these broad convenience grants with approved per-role bindings and audit
inherited access. Intended future roles include create-only ingestion and
object-viewer curation, but **neither is claimed as deployed** in Step 2.

## Credentials and verification

The upload uses the existing local Application Default Credentials through
`google.auth.default()`. Credentials remain in the user's local Google Cloud
profile; no credential JSON, access token or private key is copied into this
repository. No service-account key or HMAC key was created. A teammate must
authenticate with their own identity, never copy another member's credential.

From the repository root, inspect the live bucket and refresh committed
evidence without installing the Google Cloud CLI:

```powershell
$env:UV_CACHE_DIR = Join-Path (Get-Location) 'data/uv-cache'
uv run --locked scripts/gcs_bronze.py inspect --project data-engineering-i1 --bucket data-engineering-i1-marketplace-group
```

The script invokes `buckets.get` and `buckets.getIamPolicy`, equivalent in scope
to the bucket describe / bucket IAM commands requested in the assignment.
It exports the lifecycle rules actually read from GCS to `infra/lifecycle.json`.
The inspect action does not modify the cloud policy.
