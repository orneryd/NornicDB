# Encryption

**Protect data at rest and in transit with AES-256 encryption.**

## Overview

NornicDB provides enterprise-grade encryption to protect sensitive data:

- **At Rest**: Full database encryption using AES-256 (BadgerDB storage layer)
- **In Transit**: TLS 1.3 for network communication
- **All-or-Nothing**: When enabled, ALL data is encrypted - simple, no configuration needed
- **Strong Key Derivation**: PBKDF2 with 600,000 iterations (OWASP 2023 recommendation)

## Encryption at Rest

NornicDB uses **full database encryption** at the storage layer. When encryption is enabled:

- ✅ **ALL data is encrypted** - nodes, relationships, properties, indexes
- ✅ **Transparent operation** - no code changes needed
- ✅ **Strong protection** - AES-256 encryption
- ✅ **Simple configuration** - just enable and set password

### Configuration

```yaml
# nornicdb.yaml or ~/.nornicdb/config.yaml
database:
  encryption_enabled: true
  encryption_password: "your-secure-password-here"
```

### Environment Variables

```bash
# Set master encryption password
export NORNICDB_ENCRYPTION_PASSWORD="your-secure-password-here"

# Or configure via the Settings UI in the macOS menu bar app
```

### Code Example

```go
// Enable encryption in database config
config := nornicdb.DefaultConfig()
config.EncryptionEnabled = true
config.EncryptionPassword = os.Getenv("NORNICDB_ENCRYPTION_PASSWORD")

db, err := nornicdb.Open("/data", config)
```

## How It Works

1. **Password → Key**: Your password is converted to a 256-bit encryption key using PBKDF2
2. **Salt Generation**: A unique 32-byte salt is generated per database (stored in `data/db.salt`)
3. **Storage Encryption**: BadgerDB encrypts all data using AES-256
4. **Transparent I/O**: Encryption/decryption happens automatically on read/write

```
┌─────────────────────────────────────────────────────────┐
│                    Your Application                      │
├─────────────────────────────────────────────────────────┤
│                      NornicDB API                        │
├─────────────────────────────────────────────────────────┤
│                 BadgerDB Storage Layer                   │
│              ┌───────────────────────────┐               │
│              │    AES-256 Encryption     │               │
│              │   (automatic, transparent) │               │
│              └───────────────────────────┘               │
├─────────────────────────────────────────────────────────┤
│                    Encrypted Files                       │
│                  (unreadable without key)                │
└─────────────────────────────────────────────────────────┘
```

## Encryption in Transit (TLS)

### Server Configuration

```yaml
# Enable TLS for the HTTP listener
server:
  tls:
    enabled: true
    cert_file: /etc/nornicdb/server.crt
    key_file: /etc/nornicdb/server.key
```

For Bolt-side TLS, set `NORNICDB_BOLT_TLS_ENABLED=true` and configure the certificate files. For inter-cluster TLS see [Cluster Security](../operations/cluster-security.md). Client certificate authentication (`client_auth`) and explicit `min_version` selection are not configurable through the public YAML schema today; both default to standard library defaults.

### Generate Certificates

```bash
# Generate self-signed certificate (development)
openssl req -x509 -nodes -days 365 -newkey rsa:4096 \
  -keyout server.key -out server.crt \
  -subj "/CN=nornicdb.local"

# For production, use Let's Encrypt or your CA
```

## Important Considerations

### ⚠️ Password Security

**If you lose your encryption password, your data is PERMANENTLY LOST.**

- Store password securely (password manager, secrets vault)
- Keep a backup in a secure location
- The password cannot be recovered

### ⚠️ Migration Limitations

**You cannot convert between encrypted/unencrypted in place.**

To enable encryption on existing data:

1. Trigger a JSON backup of the live database via `POST /admin/backup` (see [Backup & Restore](../operations/backup-restore.md)).
2. Stop the server, point at a fresh data directory, and start the server with the desired encryption settings (see [CMEK Setup](../encryption/cmek-setup.md)).
3. Restore the backup file via the embedded Go API (`db.Restore`) or by replaying the backup against the fresh database.

To disable encryption, follow the same process in reverse: back up from the encrypted database, start a new instance without encryption configured, and restore.

### Error Handling

When opening an encrypted database:

| Scenario | Result |
|----------|--------|
| Correct password | ✅ Database opens normally |
| Wrong password | ❌ Error: "ENCRYPTION ERROR: Failed to open database..." |
| No password provided | ❌ Error: "Database appears to be encrypted but no password..." |
| Unencrypted DB + password | ⚠️ May fail or create new encrypted DB |

## Compliance Mapping

| Requirement | NornicDB Feature |
|-------------|------------------|
| GDPR Art.32 | AES-256 full database encryption, TLS 1.3 |
| HIPAA §164.312(a)(2)(iv) | Full database encryption protects all PHI |
| HIPAA §164.312(e)(1) | TLS for transmission security |
| SOC2 CC6.1 | Encryption key management via PBKDF2 |
| PCI-DSS 3.4 | Full encryption covers cardholder data |

## Security Best Practices

### Password Requirements

- **Minimum 16 characters** (32+ recommended)
- Use a strong, unique password
- Consider a passphrase: "correct-horse-battery-staple-nornicdb-2024"

### Key Storage

**DO:**
- Store encryption password in environment variables
- Use secret management (HashiCorp Vault, AWS Secrets Manager)
- Back up your password securely

**DON'T:**
- Hardcode passwords in configuration files
- Store passwords in version control
- Use weak or common passwords
- Share passwords via insecure channels

## Performance Considerations

Full database encryption adds minimal overhead:

| Operation | Without Encryption | With Encryption | Overhead |
|-----------|-------------------|-----------------|----------|
| Write | 45,000 ops/s | 42,000 ops/s | ~7% |
| Read | 120,000 ops/s | 110,000 ops/s | ~8% |

The overhead is acceptable for most use cases and provides complete protection.

## Troubleshooting

### Common Issues

**Error: "ENCRYPTION ERROR: Failed to open database..."**
- Cause: Wrong encryption password
- Fix: Verify `NORNICDB_ENCRYPTION_PASSWORD` or `encryption_password` in config
- ⚠️ If you forgot the password, data cannot be recovered

**Error: "Database appears to be encrypted but no password was provided"**
- Cause: Database was created with encryption, but password not set
- Fix: Set `NORNICDB_ENCRYPTION_PASSWORD` or `encryption_password` in config

**Error: "encryption key must be 16, 24, or 32 bytes"**
- Cause: Internal error (key derivation issue)
- Fix: Ensure password is not empty, check for special characters

### Checking Encryption Status

NornicDB does not expose encryption status as a structured field on `/status`. To confirm encryption is active:

- Check the server startup logs for the encryption-provider initialization line.
- Inspect the wrapped DEK metadata file at `<data_dir>/db.kms_dek.json` (provider-backed modes).
- Inspect the encryption audit log at `<data_dir>/encryption-audit.jsonl` (or the path set by `NORNICDB_ENCRYPTION_AUDIT_LOG_PATH`) for `KEY_GENERATED`, `KEY_DECRYPTED`, and `KEY_ROTATED` events.

## See Also

- **[RBAC](rbac.md)** - Access control
- **[Audit Logging](audit-logging.md)** - Compliance trails
- **[HIPAA Compliance](hipaa-compliance.md)** - Healthcare requirements
- **[CMEK Setup](../encryption/cmek-setup.md)** - Provider-backed key management
- **[HSM Integration](../encryption/hsm-integration.md)** - Deployment guidance
- **[Compliance Evidence](../encryption/compliance-evidence.md)** - Audit evidence export
