# Compliance Evidence Export

NornicDB includes:

- append-only local KMS audit archiver (`pkg/kms/audit_archiver.go`)
- compliance reporter summaries (`pkg/compliance/report.go`)
- provider audit decoration for DEK generate/decrypt/rotate operations

## Audit Log Format

JSONL, one event per line. Fields include:

- `event_type`
- `key_uri`
- `timestamp`
- `status`
- `error_code`
- `signature` (when signing is enabled)

Typical event types:

- `KEY_GENERATED`
- `KEY_DECRYPTED`
- `KEY_ROTATED`
- `KEY_METADATA_READ`

## Generate Evidence in Code

```go
reporter := compliance.NewComplianceReporter("/var/log/nornicdb/encryption-audit.jsonl")
hipaa, _ := reporter.ExportHIPAAEvidence(start, end)
soc2, _ := reporter.ExportSOC2Evidence(start, end)
```

## Default File Locations

- wrapped DEK metadata: `<data_dir>/db.kms_dek.json`
- local audit log: `<data_dir>/encryption-audit.jsonl`

Set `database.encryption_audit_log_path` or `NORNICDB_ENCRYPTION_AUDIT_LOG_PATH` to override the audit location.

## Signing and Verification

When `encryption_audit_sign_events` is enabled, NornicDB signs each local audit record with HMAC-SHA256 using `encryption_audit_sign_key`.

This gives you:

- tamper-evident local audit records,
- stable evidence input for `pkg/compliance/report.go`,
- a simple bridge for exporting signed JSONL to SIEM or object storage.

## Operational Recommendations

- keep audit logs immutable in storage policy,
- export to SIEM on schedule,
- retain per regulation (for example 6-7 years where required),
- rotate audit signing keys according to policy,
- align wrapped-DEK rotation intervals with your KEK rotation policy.
