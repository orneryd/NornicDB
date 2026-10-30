# SOC2 Compliance

**Service Organization Control 2 compliance for service providers.**

## Overview

NornicDB provides features to help organizations achieve SOC2 compliance for the Security, Availability, and Confidentiality trust service criteria.

## Trust Service Criteria

### Security (CC6)

| Criteria | Requirement | NornicDB Feature |
|----------|-------------|------------------|
| CC6.1 | Logical access controls | RBAC, JWT authentication |
| CC6.2 | Access provisioning | User management API |
| CC6.3 | Access removal | User disable/delete |
| CC6.6 | Encryption | AES-256-GCM, TLS 1.3 |
| CC6.7 | Transmission security | TLS encryption |

### Availability (A1)

| Criteria | Requirement | NornicDB Feature |
|----------|-------------|------------------|
| A1.1 | Capacity management | Metrics, monitoring |
| A1.2 | Backup procedures | Backup API |
| A1.3 | Recovery procedures | Restore API |

### Confidentiality (C1)

| Criteria | Requirement | NornicDB Feature |
|----------|-------------|------------------|
| C1.1 | Confidential data identification | Full encryption |
| C1.2 | Confidential data disposal | Secure deletion |

### Processing Integrity (PI1)

| Criteria | Requirement | NornicDB Feature |
|----------|-------------|------------------|
| PI1.1 | Processing accuracy | ACID transactions |
| PI1.4 | Error detection | Validation, checksums |

## Logical Access Controls (CC6.1)

### Authentication

JWT-based authentication is enabled by setting `NORNICDB_AUTH=admin/password` and `NORNICDB_AUTH_JWT_SECRET=<32+ chars>`. The minimum password length is configurable; password complexity rules and MFA are not currently exposed.

```yaml
auth:
  enabled: true
  username: admin
  password: "${ADMIN_PASSWORD}"
  jwt_secret: "${NORNICDB_AUTH_JWT_SECRET}"
  token_expiry: 24h
  min_password_length: 12
```

### Authorization

NornicDB ships built-in `admin`, `editor`, and `viewer` roles. Custom roles, per-database access (allowlist), and per-(role, database) read/write privileges are managed at runtime through the `/auth/roles`, `/auth/access/databases`, and `/auth/access/privileges` admin APIs (see [RBAC](rbac.md) and [Per-Database RBAC](../security/per-database-rbac.md)).

### Access Reviews

There is no `nornicdb soc2 access-review` subcommand. Generate access reviews from the audit log JSONL file (default `/var/log/nornicdb/audit.log`) and the live role/allowlist state:

```bash
# Active users and roles
curl -s http://localhost:7474/auth/users -H "Authorization: Bearer $ADMIN_TOKEN"

# Allowlist (which roles can see which databases)
curl -s http://localhost:7474/auth/access/databases -H "Authorization: Bearer $ADMIN_TOKEN"

# Per-(role, database) read/write privileges
curl -s http://localhost:7474/auth/access/privileges -H "Authorization: Bearer $ADMIN_TOKEN"

# Login activity for the review window
jq -c 'select(.event_type == "LOGIN" and .timestamp >= "2024-12-01")' \
  /var/log/nornicdb/audit.log
```

## System Monitoring (CC7.2)

### Audit Logging

```yaml
compliance:
  audit_enabled: true
  audit_log_path: /var/log/nornicdb/audit.log
  audit_retention_days: 2555  # 7 years (covers SOC2)
```

NornicDB writes structured JSONL covering authentication, authorization, data access, configuration changes, and system events. Per-event-class toggles are not exposed; the logger emits all relevant categories whenever audit logging is enabled.

### Metrics & Alerting

NornicDB exposes Prometheus-compatible metrics on the telemetry listener at `:9090/metrics` (and the legacy `/metrics` route on the data plane). Alerting thresholds are not configured inside NornicDB — define them in your Prometheus or Alertmanager rules. See [Monitoring](../operations/monitoring.md) for the metric catalog and an example alerting rule set.

### Health Checks

```bash
# Health endpoint (minimal info, no sensitive data)
curl http://localhost:7474/health
# {"status": "healthy"}

# Detailed status (requires auth)
curl http://localhost:7474/status \
  -H "Authorization: Bearer $TOKEN"
```

## Change Management (CC8.1)

### Configuration Control

NornicDB does not annotate its YAML config with version metadata. Track configuration changes through your existing change-management process (Git history of the YAML, infrastructure-as-code review, etc.) and confirm the live state via `GET /admin/config` (admin permission required). Configuration changes also produce `CONFIG_CHANGE` events in the audit log when the relevant subsystem mutates settings at runtime.

### Change Logging

```json
{
  "timestamp": "2024-12-01T10:00:00Z",
  "type": "CONFIG_CHANGE",
  "user_id": "admin",
  "setting": "auth.session_timeout",
  "old_value": "30m",
  "new_value": "15m",
  "approved_by": "security-team"
}
```

## Risk Management (CC3.1)

### Security Defaults

NornicDB ships with secure defaults applied at startup (`pkg/config.LoadDefaults` and `pkg/server`):

- Bind address: `127.0.0.1` (localhost only) — set `NORNICDB_ADDRESS=0.0.0.0` to expose externally.
- CORS: disabled.
- IP-based rate limiting: enabled (default 100 req/min, 3000 req/hour per IP, burst 10).
- Storage encryption: opt-in (set `NORNICDB_ENCRYPTION_PROVIDER` and the relevant key material to enable; see [Encryption](encryption.md)).
- Auth: opt-in (set `NORNICDB_AUTH=user/pass` and `NORNICDB_AUTH_JWT_SECRET`).

### Rate Limiting

Rate-limit defaults are set in code (`pkg/server`) and not currently exposed through public YAML. The defaults apply per IP: `100` requests/minute, `3000` requests/hour, `10` burst. To customise rate limiting in embedded deployments, set `Server.RateLimitEnabled`, `RateLimitPerMinute`, and `RateLimitPerHour` on the server config struct before starting the server.

## Backup & Recovery (A1.2, A1.3)

See [Backup & Restore](../operations/backup-restore.md) for the canonical procedure. Backups are produced through the `/admin/backup` HTTP endpoint or by snapshotting the data directory while the server is stopped. There is no `nornicdb backup`/`nornicdb restore` CLI subcommand; restoration is performed via the embedded Go API (`db.Restore`) or by copying the backup file into the data directory at startup.

### Backup Procedures

```bash
# Trigger an admin backup over HTTP
curl -sf -X POST http://localhost:7474/admin/backup \
  -H "Authorization: Bearer $TOKEN" \
  -d "{\"output\":\"/backups/backup-$(date +%Y%m%d).json\"}"
```

### Recovery Procedures

For embedded deployments, call `db.Restore(ctx, path)` from your application code. For server deployments, stop the server, replace the data directory with the desired state (or copy in the JSON backup file), and start the server.

Point-in-time recovery is not exposed as a single command; combine WAL retention (see [WAL Compaction](../operations/wal-compaction.md)) with MVCC historical reads (see [Historical Reads & MVCC Retention](../user-guides/historical-reads-mvcc-retention.md)).

### Recovery Testing

Use a non-production environment to validate restores. The MVCC lifecycle admin API (`GET /admin/databases/{db}/mvcc`) reports lifecycle state after a restore so operators can confirm the target's MVCC head metadata is healthy before promoting the instance.

## Encryption (CC6.6, CC6.7)

### At Rest

```yaml
encryption:
  enabled: true
  algorithm: AES-256-GCM
  key_rotation_days: 90
```

### In Transit

```yaml
tls:
  enabled: true
  min_version: TLS1.2
  cert_file: /etc/nornicdb/server.crt
  key_file: /etc/nornicdb/server.key
```

## Compliance Reporting

### Generate SOC2 Report

```bash
# Generate SOC2 evidence report
nornicdb soc2 report \
  --period "2024-01-01:2024-12-31" \
  --output soc2-evidence-2024.pdf

# Include specific controls
nornicdb soc2 report \
  --controls CC6.1,CC6.6,CC7.2 \
  --output access-controls-report.pdf
```

### Report Contents

- User access reviews
- Authentication logs
- Configuration changes
- Security incidents
- Backup verification
- System uptime metrics

## Control Evidence

### CC6.1 - Access Control Evidence

```sql
-- Active users and roles
MATCH (u:User) RETURN u.username, u.roles, u.last_login

-- Failed login attempts
MATCH (e:AuditEvent {type: 'LOGIN_FAILED'})
RETURN e.username, e.ip_address, e.timestamp
```

### CC7.2 - Monitoring Evidence

```bash
# System metrics
curl http://localhost:7474/metrics -H "Authorization: Bearer $TOKEN"

# Audit log summary (last month)
jq -s 'group_by(.event_type) | map({event_type: .[0].event_type, count: length})' \
  /var/log/nornicdb/audit.log
```

### CC8.1 - Change Evidence

Configuration changes are recorded in the audit log via `event_type=CONFIG_CHANGE`. Pull configuration history with:

```bash
jq -c 'select(.event_type == "CONFIG_CHANGE")' /var/log/nornicdb/audit.log
```

The current effective configuration is exposed at `GET /admin/config` (admin permission required).

## Compliance Checklist

### Security Controls

- [ ] Enable authentication
- [ ] Configure RBAC
- [ ] Enable TLS encryption
- [ ] Enable audit logging
- [ ] Configure rate limiting
- [ ] Set up security alerting

### Operational Controls

- [ ] Configure monitoring
- [ ] Set up backup procedures
- [ ] Test recovery procedures
- [ ] Document change management
- [ ] Conduct access reviews

### Documentation

- [ ] Security policies
- [ ] Access control matrix
- [ ] Incident response plan
- [ ] Business continuity plan
- [ ] Change management procedures

## See Also

- **[Encryption](encryption.md)** - Data protection
- **[RBAC](rbac.md)** - Access control
- **[Audit Logging](audit-logging.md)** - Monitoring
- **[HIPAA Compliance](hipaa-compliance.md)** - Healthcare
- **[GDPR Compliance](gdpr-compliance.md)** - EU requirements

