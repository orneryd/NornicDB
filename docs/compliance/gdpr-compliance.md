# GDPR Compliance

NornicDB provides GDPR-oriented export, erasure, anonymization, consent, audit, and access-control features. This guide documents the current runtime behavior and the current configuration surface.

## Supported Areas

| Article | Requirement | NornicDB Feature |
|---------|-------------|------------------|
| Art.15 | Right of Access | `POST /gdpr/export` |
| Art.16 | Right to Rectification | Standard update APIs |
| Art.17 | Right to Erasure | `POST /gdpr/delete` |
| Art.20 | Data Portability | JSON or CSV export |
| Art.25 | Privacy by Design | Configurable anonymization and retention controls |
| Art.30 | Records of Processing | Audit logging |
| Art.32 | Security of Processing | RBAC, session controls, encryption settings |

## Configuration

The relevant GDPR and compliance controls live under `compliance:`.

```yaml
compliance:
  audit_enabled: true
  audit_log_path: "./logs/audit.log"

  data_export_enabled: true
  data_erasure_enabled: true
  data_access_enabled: true

  anonymization_enabled: true
  anonymization_method: "pseudonymization"

  access_control_enabled: true
  session_timeout: "30m"
  max_failed_logins: 5
  lockout_duration: "15m"

  encryption_at_rest: false
  encryption_in_transit: true

  subject_identifier_properties: [owner_id, user_id, account_id, created_by]
  subject_pseudonymize_properties: [owner_id, user_id, account_id, created_by]
  subject_redact_properties: [email, name, username, ip_address]

  retention_enabled: false
  retention_policy_days: 0
  retention_auto_delete: false
  retention_exempt_roles: [admin]
```

### Subject Matching

GDPR export, delete, and anonymize flows no longer assume a single schema-specific property such as `owner_id`.

- `subject_identifier_properties` controls how a node is matched to a subject.
- `subject_pseudonymize_properties` controls which properties are replaced with anonymized tokens.
- `subject_redact_properties` controls which properties are removed entirely during anonymization.

This lets the same runtime work across different data models without code changes.

### Retention Interaction

Retention is separate from GDPR, but the two are integrated when retention is enabled.

- `retention_enabled` is disabled by default and fully opt-in.
- if retention is enabled, `POST /gdpr/delete` checks legal holds before delete/anonymize
- if retention is enabled, `POST /gdpr/delete` creates a tracked erasure request

See [Retention Policies](../user-guides/retention-policies.md).

## Right of Access (Art.15)

Use `POST /gdpr/export`.

```bash
curl -X POST http://localhost:7474/gdpr/export \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"user_id":"user-123","format":"json"}'
```

Supported formats:

- `json`
- `csv`

The caller can export only their own data unless they have admin permission.

## Right to Erasure (Art.17)

Use `POST /gdpr/delete`.

Hard delete:

```bash
curl -X POST http://localhost:7474/gdpr/delete \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"user_id":"user-123","confirm":true}'
```

Anonymize instead of delete:

```bash
curl -X POST http://localhost:7474/gdpr/delete \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"user_id":"user-123","confirm":true,"anonymize":true}'
```

Current response shape:

```json
{
  "status": "deleted",
  "user_id": "user-123"
}
```

Or, when anonymizing:

```json
{
  "status": "anonymized",
  "user_id": "user-123"
}
```

If retention-backed legal holds are active, the endpoint returns `409 Conflict`.

## Data Portability (Art.20)

Use the same export endpoint with the requested format.

```bash
curl -X POST http://localhost:7474/gdpr/export \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"user_id":"user-123","format":"csv"}'
```

## Anonymization Behavior

`db.AnonymizeUserData()` uses the configured subject fields rather than hard-coded schema assumptions.

- pseudonymized fields get replacement values
- redacted fields are removed
- graph shape is preserved for analytics and relationship continuity

## Consent Management

Consent APIs remain available in code and can be used alongside GDPR workflows.

```go
err := db.RecordConsent(ctx, &nornicdb.Consent{
	UserID:  "user-123",
	Purpose: "marketing",
	Given:   true,
	Source:  "web_form",
})
```

## Audit And Security Controls

GDPR workflows rely on the broader compliance controls:

- audit logging via `audit_enabled` and `audit_log_path`
- authentication and RBAC around `/gdpr/export` and `/gdpr/delete`
- session timeout and lockout controls
- encryption-in-transit and at-rest settings

See [Audit Logging](audit-logging.md), [RBAC](rbac.md), and [Encryption](encryption.md).

## Practical Checklist

- enable audit logging
- confirm `data_export_enabled` and `data_erasure_enabled` are set appropriately
- configure subject identifier, pseudonymize, and redact properties for your schema
- enforce RBAC and session controls
- decide whether retention is enabled and whether legal holds must participate in erasure
- test both delete and anonymize flows with representative production-shaped data

## See Also

- [Retention Policies](../user-guides/retention-policies.md)
- [Audit Logging](audit-logging.md)
- [RBAC](rbac.md)
- [HIPAA Compliance](hipaa-compliance.md)

