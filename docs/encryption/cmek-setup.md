# CMEK Setup Guide

NornicDB supports provider-backed data-at-rest keys by generating a Badger data-encryption key (DEK), wrapping that DEK with a configured provider key-encryption key (KEK), and persisting only the wrapped DEK metadata on disk.

## Provider Modes

- `password` (default): PBKDF2-derived Badger key from `encryption_password`
- `local`: provider-backed DEK wrapping with a local KEK (dev/test)
- `aws-kms`: AWS KMS-backed wrapping
- `azure-keyvault`: Azure Key Vault-backed wrapping
- `gcp-cloudkms`: GCP Cloud KMS-backed wrapping

## Runtime Behavior

- NornicDB stores the wrapped DEK metadata in `<data_dir>/db.kms_dek.json`
- The plaintext DEK is used only in memory to initialize Badger at startup
- Provider-backed audit events are appended to `<data_dir>/encryption-audit.jsonl` unless `encryption_audit_log_path` is set
- Wrapped-DEK rotation rewraps the persisted DEK metadata with the active provider key; it does not hot-swap the live Badger DEK

## Minimal YAML

```yaml
database:
  encryption_enabled: true
  encryption_provider: "local"
  encryption_key_uri: "kms://local/nornicdb"
  encryption_master_key: "replace-with-32-byte-test-placeholder"
  encryption_audit_sign_events: true
  encryption_audit_sign_key: "replace-with-hmac-signing-key"
  encryption_rotation_enabled: true
  encryption_rotation_interval: "2160h"
```

## AWS KMS Example

```yaml
database:
  encryption_enabled: true
  encryption_provider: "aws-kms"
  encryption_aws_region: "us-east-1"
  encryption_aws_kms_key_id: "arn:aws:kms:us-east-1:123456789012:key/..."
  encryption_aws_role_arn: "arn:aws:iam::123456789012:role/nornicdb-kms"
  encryption_aws_role_session_name: "nornicdb"
```

Equivalent env vars:

- `NORNICDB_ENCRYPTION_ENABLED=true`
- `NORNICDB_ENCRYPTION_PROVIDER=aws-kms`
- `NORNICDB_ENCRYPTION_AWS_REGION=us-east-1`
- `NORNICDB_ENCRYPTION_AWS_KMS_KEY_ID=...`
- `NORNICDB_ENCRYPTION_ROTATION_ENABLED=true`
- `NORNICDB_ENCRYPTION_ROTATION_INTERVAL=2160h`

## Azure Key Vault Example

```yaml
database:
  encryption_enabled: true
  encryption_provider: "azure-keyvault"
  encryption_azure_vault_name: "nornicdb-prod-kv"
  encryption_azure_key_name: "nornicdb-master-key"
  encryption_azure_tenant_id: "..."
  encryption_azure_client_id: "..."
  encryption_azure_client_secret: "..."
```

## GCP Cloud KMS Example

```yaml
database:
  encryption_enabled: true
  encryption_provider: "gcp-cloudkms"
  encryption_gcp_project: "my-project"
  encryption_gcp_location: "us-central1"
  encryption_gcp_key_ring: "nornicdb"
  encryption_gcp_key_name: "storage-kek"
  encryption_gcp_credentials_file: "/secrets/gcp-kms.json"
```

## Audit and Rotation Settings

```yaml
database:
  encryption_audit_log_path: "/var/log/nornicdb/encryption-audit.jsonl"
  encryption_audit_sign_events: true
  encryption_audit_sign_key: "replace-with-hmac-signing-key"
  encryption_rotation_enabled: true
  encryption_rotation_interval: "2160h"
```

Environment variables:

- `NORNICDB_ENCRYPTION_AUDIT_LOG_PATH`
- `NORNICDB_ENCRYPTION_AUDIT_SIGN_EVENTS`
- `NORNICDB_ENCRYPTION_AUDIT_SIGN_KEY`
- `NORNICDB_ENCRYPTION_ROTATION_ENABLED`
- `NORNICDB_ENCRYPTION_ROTATION_INTERVAL`

## What Is Persisted

When provider-backed encryption is enabled, NornicDB stores encrypted DEK metadata at:

- `<data_dir>/db.kms_dek.json`
- `<data_dir>/encryption-audit.jsonl` by default, or `encryption_audit_log_path` when configured

This file stores only wrapped DEK metadata (never plaintext DEK).

## Validation Rules

- `password` mode requires `encryption_password`
- `local` mode requires `encryption_master_key`
- `aws-kms` requires `encryption_aws_region` and `encryption_aws_kms_key_id`
- `azure-keyvault` requires `encryption_azure_vault_name` and `encryption_azure_key_name`
- `gcp-cloudkms` requires `encryption_gcp_project`, `encryption_gcp_location`, `encryption_gcp_key_ring`, and `encryption_gcp_key_name`
- enabling audit signing requires `encryption_audit_sign_key`
