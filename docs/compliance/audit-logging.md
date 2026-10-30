# Audit Logging

**Comprehensive audit trails for compliance and security monitoring.**

## Overview

NornicDB provides immutable audit logging required by major regulatory frameworks:

- **GDPR Art.30** - Records of processing activities
- **HIPAA §164.312(b)** - Audit controls
- **SOC2 CC7.2** - System monitoring
- **FISMA AU-2** - Audit events

## Features

- ✅ Immutable append-only logs
- ✅ Structured JSON format
- ✅ Real-time security alerting
- ✅ Compliance reporting
- ✅ Configurable retention (7+ years)
- ✅ User activity tracking
- ✅ Data access logging

## Configuration

### Enable Audit Logging

Audit settings live under `compliance:`:

```yaml
# nornicdb.yaml
compliance:
  audit_enabled: true
  audit_log_path: "/var/log/nornicdb/audit.log"
  audit_retention_days: 2555  # ~7 years (covers SOC2 7-year requirement)
```

NornicDB does not expose per-event-class toggles (`log_queries`, `log_data_access`, etc.) or declarative alerting thresholds in configuration. The audit logger emits structured JSONL for every relevant event; route the file into your existing SIEM/detection pipeline if you need alerting on patterns like repeated failed logins.

### Code Example

```go
// Initialize audit logger
config := audit.DefaultConfig()
config.LogPath = "/var/log/nornicdb/audit.log"
config.RetentionDays = 2555

logger, err := audit.NewLogger(config)
if err != nil {
    log.Fatal(err)
}
defer logger.Close()

// Set up security alerting
logger.SetAlertCallback(func(event audit.Event) {
    if event.Type == audit.EventSecurityAlert {
        sendSecurityAlert(event)
    }
})

// Attach to server
server.SetAuditLogger(logger)
```

## Event Types

### Authentication Events

| Event Type | Description |
|------------|-------------|
| `LOGIN` | Successful login |
| `LOGIN_FAILED` | Failed login attempt |
| `LOGOUT` | User logout |
| `PASSWORD_CHANGE` | Password changed |
| `ACCESS_DENIED` | Authorization failure |

### Data Events (GDPR Art.15)

| Event Type | Description |
|------------|-------------|
| `DATA_READ` | Data accessed |
| `DATA_CREATE` | Data created |
| `DATA_UPDATE` | Data modified |
| `DATA_DELETE` | Data deleted |
| `DATA_EXPORT` | Data exported |

### GDPR Rights Events

| Event Type | Description |
|------------|-------------|
| `ERASURE_REQUEST` | Right to be forgotten request |
| `ERASURE_COMPLETE` | Erasure completed |
| `EXPORT_REQUEST` | Data portability request |
| `CONSENT_GIVEN` | Consent recorded |
| `CONSENT_REVOKED` | Consent withdrawn |

### System Events

| Event Type | Description |
|------------|-------------|
| `CONFIG_CHANGE` | Configuration modified |
| `BACKUP` | Backup created |
| `RESTORE` | Backup restored |
| `SECURITY_ALERT` | Security event detected |

## Log Format

### JSON Structure

```json
{
  "id": "evt_abc123xyz",
  "timestamp": "2024-12-01T10:30:00.123Z",
  "type": "DATA_READ",
  "user_id": "usr_123",
  "username": "alice",
  "ip_address": "192.168.1.100",
  "user_agent": "Mozilla/5.0...",
  "resource": "node",
  "resource_id": "patient-456",
  "action": "READ",
  "success": true,
  "details": "PHI access",
  "session_id": "sess_789"
}
```

### Fields

| Field | Description | Required |
|-------|-------------|----------|
| `id` | Unique event ID | Yes |
| `timestamp` | ISO 8601 timestamp | Yes |
| `type` | Event type | Yes |
| `user_id` | User identifier | Yes |
| `username` | Human-readable name | No |
| `ip_address` | Client IP | Yes |
| `resource` | Object type accessed | For data events |
| `resource_id` | Object identifier | For data events |
| `action` | Operation performed | For data events |
| `success` | Operation result | Yes |
| `details` | Additional context | No |

## Compliance Reporting

### Generate Reports

```go
// Create audit reader
reader := audit.NewReader(config.LogPath)

// Generate compliance report
report, err := reader.GenerateComplianceReport(
    time.Now().AddDate(0, -1, 0), // Start: 1 month ago
    time.Now(),                    // End: now
    "Monthly Compliance Report",
)

fmt.Printf("Total events: %d\n", report.TotalEvents)
fmt.Printf("Failed logins: %d\n", report.FailedLogins)
fmt.Printf("Data accesses: %d\n", report.DataAccesses)
fmt.Printf("GDPR requests: %d\n", report.GDPRRequests)
```

### Searching Audit Logs

NornicDB writes audit events as JSON Lines to the configured `log_path` (default `/var/log/nornicdb/audit.log`). Use standard tooling such as `jq`, `grep`, or your log-aggregation pipeline to query them:

```bash
# Generate compliance report (last month)
jq -c 'select(.timestamp >= "2024-11-01" and .timestamp < "2024-12-01")' \
  /var/log/nornicdb/audit.log

# Export for external analysis (CSV)
jq -r '[.timestamp, .event_type, .username, .ip_address, .success] | @csv' \
  /var/log/nornicdb/audit.log > audit-november.csv

# Search for specific events
jq -c 'select(.username == "alice" and .event_type == "LOGIN_FAILED")' \
  /var/log/nornicdb/audit.log
```

## Security Alerting

### Configure Alerts

```go
logger.SetAlertCallback(func(event audit.Event) {
    switch event.Type {
    case audit.EventLoginFailed:
        if getFailedLoginCount(event.IPAddress) >= 5 {
            sendSlackAlert("Multiple failed logins from " + event.IPAddress)
        }
    case audit.EventSecurityAlert:
        sendPagerDutyAlert(event)
    case audit.EventErasureRequest:
        notifyDPO(event) // Notify Data Protection Officer
    }
})
```

### Alert Conditions

| Condition | Default Threshold | Action |
|-----------|-------------------|--------|
| Failed logins | 5 in 15 minutes | Alert + lockout |
| Unusual data access | N/A | Alert |
| Config changes | Any | Alert |
| GDPR requests | Any | Notify DPO |

## Log Rotation

### Automatic Rotation

The audit logger rotates automatically based on file size and elapsed time. Defaults: 100 MB rotation size, 24-hour rotation interval. These are configurable through the embedded Go API on `audit.Config`:

```go
config := audit.DefaultConfig()
config.RotationSize = 100 * 1024 * 1024 // 100 MB
config.RotationInterval = 24 * time.Hour
config.RetentionDays = 2555
config.SyncWrites = true
```

There is no public YAML for rotation today; the file-size and interval values come from the embedded defaults unless your application code overrides them.

### Manual Rotation

Audit log rotation is handled by the configured rotation policy (`max_size`, `max_age`, `max_backups`, `compress` under the `audit:` block). For ad-hoc archival, use standard log tooling:

```bash
# Archive logs older than a date
find /var/log/nornicdb -name 'audit.log.*' -newermt "2023-01-01" ! -newermt "2024-01-01" \
  -print0 | tar -czvf archive-2023.tar.gz --null -T -
```

## Retention Management

### GDPR Requirements
- Keep logs as long as necessary for purpose
- Delete when no longer needed

### HIPAA Requirements
- Minimum 6 years retention
- Recommend 7+ years

### SOC2 Requirements
- 7 years recommended

```yaml
# Configure retention
audit:
  retention_days: 2555  # 7 years
  auto_purge: true      # Delete expired logs
```

## Integration

### Syslog

```yaml
audit:
  syslog:
    enabled: true
    address: "syslog.example.com:514"
    facility: local0
```

### Elasticsearch

```yaml
audit:
  elasticsearch:
    enabled: true
    urls: ["https://es.example.com:9200"]
    index: "nornicdb-audit"
```

### Splunk

```yaml
audit:
  splunk:
    enabled: true
    hec_url: "https://splunk.example.com:8088"
    token: "${SPLUNK_HEC_TOKEN}"
```

## Best Practices

### DO:
- Enable audit logging in production
- Set up alerting for security events
- Regularly review audit logs
- Keep logs for compliance period
- Encrypt log files at rest

### DON'T:
- Disable audit logging
- Delete logs before retention period
- Log sensitive data in details field
- Ignore security alerts

## See Also

- **[RBAC](rbac.md)** - Access control
- **[Encryption](encryption.md)** - Data protection
- **[HIPAA Compliance](hipaa-compliance.md)** - Healthcare requirements
- **[GDPR Compliance](gdpr-compliance.md)** - EU data protection

