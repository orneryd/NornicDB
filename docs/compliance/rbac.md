# Role-Based Access Control (RBAC)

**Define roles, assign entitlements, and control per-database access.**

Last Updated: April 2026

---

## Overview

NornicDB implements a layered RBAC system:

1. **Roles** — Built-in (admin, editor, viewer) and user-defined custom roles
2. **Global entitlements** — Permissions that apply server-wide (read, write, admin, schema, etc.)
3. **Per-database access** — Control which roles can see and access each database
4. **Per-database privileges** — Fine-grained read/write control per (role, database) pair
5. **JWT authentication** — Stateless token-based auth with password policies and account lockout

---

## Roles

### Built-in Roles

| Role | Entitlements | Description |
|---|---|---|
| `admin` | read, write, create, delete, admin, schema, user_manage | Full access and system administration |
| `editor` | read, write, create, delete | Read and write data |
| `viewer` | read | Read-only access |
| `none` | — | No access (disabled account) |

Built-in roles cannot be renamed or deleted.

### Custom Roles

Create custom roles to model your organization's access patterns. Custom roles start with no global entitlements — assign per-database privileges to control what they can do.

**List all roles:**

```bash
curl -X GET http://localhost:7474/auth/roles \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

Response:

```json
["admin", "editor", "viewer", "analyst", "auditor"]
```

**Create a custom role:**

```bash
curl -X POST http://localhost:7474/auth/roles \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "analyst"}'
```

**Rename a custom role:**

```bash
curl -X PATCH http://localhost:7474/auth/roles/analyst \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "data_analyst"}'
```

Renaming a role automatically updates the allowlist and all user assignments.

**Delete a custom role:**

```bash
curl -X DELETE http://localhost:7474/auth/roles/data_analyst \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

A role cannot be deleted while any user is assigned to it.

---

## Entitlements

Entitlements are the individual permissions that can be assigned to roles. Query the full canonical list from the API:

```bash
curl -X GET http://localhost:7474/auth/entitlements \
  -H "Authorization: Bearer $TOKEN"
```

### Global Entitlements

These apply server-wide and are assigned to roles by default (built-in roles) or via configuration (custom roles).

| ID | Name | What It Controls |
|---|---|---|
| `read` | Read | Cypher MATCH, search, metrics, status, Bifrost/GraphQL reads, MCP reads |
| `write` | Write | Cypher CREATE/DELETE/SET/MERGE, registered `MODE WRITE` procedures, embed triggers, search rebuild, GraphQL mutations |
| `create` | Create | Resource creation operations |
| `delete` | Delete | Delete operations (e.g. GDPR delete) |
| `admin` | Admin | Role management, database access/privileges, backup, GPU config, system administration, registered `MODE ADMIN` and `MODE DBMS` procedures |
| `schema` | Schema | CREATE/DROP INDEX and CONSTRAINT via Bolt, registered `MODE SCHEMA` procedures |
| `user_manage` | User Management | Create, update, delete user accounts (distinct from admin) |

### Per-Database Entitlements

These apply per database and are controlled through the allowlist and privileges APIs.

| ID | Name | What It Controls |
|---|---|---|
| `database_see` | Database: See | Database appears in `SHOW DATABASES` and the catalogue |
| `database_access` | Database: Access | Role can run queries against this database |
| `database_read` | Database: Read | Read operations (MATCH, property reads) on this database |
| `database_write` | Database: Write | Write operations (CREATE, DELETE, SET, MERGE) on this database |

---

## Per-Database Access Control

### Database Allowlist

The allowlist controls which roles can **see** and **access** which databases. If no allowlist is configured for a role, that role can access **all databases** (empty list = all).

**View current allowlist:**

```bash
curl -X GET http://localhost:7474/auth/access/databases \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

Response:

```json
[
  {"role": "analyst", "databases": ["analytics", "reporting"]},
  {"role": "viewer", "databases": ["public_data"]}
]
```

**Set allowlist for a single role:**

```bash
curl -X PUT http://localhost:7474/auth/access/databases \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "role": "analyst",
    "databases": ["analytics", "reporting"]
  }'
```

**Set allowlist for multiple roles at once:**

```bash
curl -X PUT http://localhost:7474/auth/access/databases \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "mappings": [
      {"role": "analyst", "databases": ["analytics", "reporting"]},
      {"role": "auditor", "databases": ["audit_logs"]}
    ]
  }'
```

### Per-Database Privileges

The privileges matrix provides fine-grained **read** and **write** control per (role, database) pair. When no privilege entry exists for a combination, access falls back to the role's global permissions (e.g. editor = read+write, viewer = read-only).

**View current privileges:**

```bash
curl -X GET http://localhost:7474/auth/access/privileges \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

Response:

```json
[
  {"role": "analyst", "database": "analytics", "read": true, "write": false},
  {"role": "analyst", "database": "reporting", "read": true, "write": true},
  {"role": "auditor", "database": "audit_logs", "read": true, "write": false}
]
```

**Set privileges:**

```bash
curl -X PUT http://localhost:7474/auth/access/privileges \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '[
    {"role": "analyst", "database": "analytics", "read": true, "write": false},
    {"role": "analyst", "database": "reporting", "read": true, "write": true}
  ]'
```

### How Access Is Resolved

When a user queries a database, NornicDB resolves access in this order:

1. **Allowlist check** — Can this user's role(s) see and access this database?
2. **Privileges check** — Is there a (role, database) privilege entry? If yes, use it.
3. **Global fallback** — If no privilege entry, use the role's global permissions (admin/editor = read+write, viewer = read-only).

When a new database is created, the **admin** role and the creating user's roles are automatically granted full access.

---

## User Management

### Create Users

```bash
curl -X POST http://localhost:7474/auth/users \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "alice",
    "password": "SecurePass123!",
    "roles": ["analyst"]
  }'
```

Response:

```json
{
  "username": "alice",
  "email": "alice@localhost",
  "roles": ["analyst"],
  "created_at": "2026-04-01T10:30:00Z",
  "disabled": false
}
```

### Update User Roles

```bash
curl -X PUT http://localhost:7474/auth/users/alice \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"roles": ["analyst", "viewer"]}'
```

### List Users

```bash
curl -X GET http://localhost:7474/auth/users \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

### Disable a User

```bash
curl -X PUT http://localhost:7474/auth/users/alice \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"disabled": true}'
```

### Delete a User

```bash
curl -X DELETE http://localhost:7474/auth/users/alice \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

### User Profile (Self-Service)

Users can update their own password and profile without admin privileges:

```bash
# Change password
curl -X POST http://localhost:7474/auth/password \
  -H "Authorization: Bearer $USER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "old_password": "OldPass123!",
    "new_password": "NewSecurePass456!"
  }'

# Update profile metadata
curl -X PUT http://localhost:7474/auth/profile \
  -H "Authorization: Bearer $USER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "alice@example.com",
    "metadata": {"department": "Engineering", "team": "Data"}
  }'
```

### User Storage

User accounts are stored as nodes in the **system database**:

- Persisted immediately on create/update/delete
- Loaded automatically on server startup
- Included in database backups
- Internal database IDs and password hashes are never exposed in API responses

---

## Authentication

### Login (Get Token)

```bash
curl -X POST http://localhost:7474/auth/token \
  -d "grant_type=password&username=alice&password=SecurePass123!"
```

Response:

```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIs...",
  "token_type": "Bearer",
  "expires_in": 86400
}
```

### Using Tokens

```bash
# Authorization header
curl http://localhost:7474/db/nornic/tx/commit \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIs..."

# Or API key header
curl http://localhost:7474/db/nornic/tx/commit \
  -H "X-API-Key: ndb_sk_abc123..."
```

### API Keys

For service-to-service communication, generate an API token via the admin endpoint (requires the `admin` permission):

```bash
curl -X POST http://localhost:7474/auth/api-token \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "backend-service", "role": "editor"}'
```

---

## Security Features

### Account Lockout

After repeated failed login attempts, accounts are locked automatically. The lockout threshold and duration use built-in defaults (`pkg/auth/auth.go`); they are not exposed through configuration.

### Password Policy

The minimum password length is configurable via `auth.min_password_length` (default 8). Complexity rules (uppercase / number / special-character requirements) are not exposed through configuration today — passwords are validated only against the minimum length.

```yaml
auth:
  min_password_length: 12
```

Passwords are hashed using **bcrypt** with automatic salt generation. Plain-text passwords are never stored.

### Session Management

Tokens can be revoked (logout) by adding them to a server-side blacklist. Revoked tokens are rejected on subsequent requests.

---

## Configuration

```yaml
auth:
  enabled: true
  username: admin
  password: "${ADMIN_PASSWORD}"
  jwt_secret: "${NORNICDB_AUTH_JWT_SECRET}"  # Min 32 characters
  token_expiry: 24h
  min_password_length: 12
```

```bash
# Required: JWT signing secret (min 32 characters)
export NORNICDB_AUTH_JWT_SECRET="your-super-secret-jwt-key-min-32-chars"

# Optional: Disable auth for development
export NORNICDB_AUTH=none
```

The configurable surface is intentionally small: the auth block above (`enabled`, `username`, `password`, `min_password_length`, `token_expiry`, `jwt_secret`) is the full set. Bcrypt cost, password complexity rules, and lockout thresholds are not exposed through configuration today; they default to the values in `pkg/auth/auth.go`.

---

## Endpoint Protection

| Endpoint | Required Permission |
|---|---|
| `GET /health` | None (public) |
| `GET /status` | read |
| `GET /metrics` | read |
| `POST /db/{db}/tx/commit` | read or write (per-database) |
| `POST /nornicdb/search` | read |
| `POST /auth/users` | user_manage |
| `GET/POST /auth/roles` | admin |
| `GET/PUT /auth/access/databases` | admin |
| `GET/PUT /auth/access/privileges` | admin |
| `GET /auth/entitlements` | read |
| `DELETE /nornicdb/gdpr/*` | admin |
| `GET /admin/*` | admin |

---

## Audit Integration

All authentication and access events are logged:

```json
{
  "timestamp": "2026-04-01T10:30:00Z",
  "event_type": "LOGIN",
  "user_id": "usr_abc123",
  "username": "alice",
  "ip_address": "192.168.1.100",
  "user_agent": "Mozilla/5.0...",
  "success": true
}
```

See **[Audit Logging](audit-logging.md)** for full details.

---

## Compliance Mapping

| Requirement | NornicDB Feature |
|---|---|
| GDPR Art. 32 | Access controls, authentication |
| HIPAA §164.312(a)(1) | Unique user identification |
| HIPAA §164.312(d) | Person or entity authentication |
| FISMA AC-2 | Account management |
| SOC2 CC6.1 | Logical access controls |

---

## See Also

- **[Entitlements Reference](../security/entitlements.md)** — Canonical list of all assignable entitlements
- **[Per-Database RBAC & Lockout Recovery](../security/per-database-rbac.md)** — Allowlist details and admin lockout recovery
- **[Encryption](encryption.md)** — Data protection at rest
- **[Audit Logging](audit-logging.md)** — Access and event audit trails
- **[HIPAA Compliance](hipaa-compliance.md)** — Healthcare compliance mapping
- **[Multi-Database Guide](../user-guides/multi-database.md#system-database)** — System database and user storage
