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

Cypher `DROP ROLE data_analyst` on the system database does the same.

Deleting a role, over HTTP or with `DROP ROLE`, removes everything the role owns — its database access
and privileges, entitlements, procedure grants, impersonation privileges and graph privileges (collection rules
included) — and removes it from every user assigned to it, in one transaction. Those users keep their other roles. Built-in roles cannot be deleted.
Renaming a role moves the same records and assignments to the new name.

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
| `admin` | Admin | Role management, database access/privileges, backup, GPU config, system administration, and the procedures `SHOW PROCEDURES` lists as `admin` (for example `db.stats.*`, `dbms.listConfig`, `db.clearQueryCaches`, `db.cdc.query`, `db.txlog.*`; a registered `MODE ADMIN` or `MODE DBMS` procedure is one of them) |
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

The privileges matrix provides fine-grained **read** and **write** control per (role, database) pair, within the role's global entitlements: reading a database needs the global `read` entitlement, and writing it the `write` entitlement too. When none of a principal's roles has a privilege entry for a database, its global entitlements decide alone (the ones stored with `PUT /auth/role-entitlements`, else the built-in defaults: editor = read+write, viewer = read-only).

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

### Procedure Privileges

A procedure that needs administrator rights (the change-feed procedure `db.cdc.query`, for example) can be granted to a role with Neo4j's statement, so that a non-admin login can run it without other administrator rights:

```cypher
// On the system database, as an administrator
GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO cdc_reader
REVOKE EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS FROM cdc_reader
SHOW PRIVILEGES
SHOW ROLE cdc_reader PRIVILEGES
```

- A procedure is named exactly or with a glob: `*` stands for any characters, dots included, and `?` for one. Several names and several roles can be listed, separated by commas.
- `BOOSTED` lets the role run the matching procedures even when they need administrator rights. Ordinary `EXECUTE PROCEDURE`, `EXECUTE ADMIN`, `EXECUTE FUNCTION` (including boosted functions), and setting grants are refused with `Neo.ClientError.Statement.UnsupportedAdministrationCommand`; they are never accepted without enforcement. These procedure privileges are administered through Cypher only.
- The grant doesn't widen database access. The role still needs the global `read` entitlement and read access to each database the procedure reads; give it read access only on that database to keep it to one.
- `GRANT` needs the role to exist (built-in, or created with `CREATE ROLE`); `REVOKE` from a role that doesn't exist changes nothing. Only administrators may run `GRANT`, `REVOKE` and `SHOW … PRIVILEGES`, except a user listing their own (`SHOW USER PRIVILEGES`).
- Grants are read from the system database on every check, so a change applies at once to every server over the same storage. In an explicit transaction, `GRANT` and `REVOKE` take effect when it commits and are undone by a rollback. They run on the system database whichever database the transaction was opened on, as in Neo4j: an administration command as a transaction's first statement makes it an administration transaction, and after a data statement (or in a statement a procedure such as `apoc.cypher.run` runs) it fails with `Neo.ClientError.Transaction.ForbiddenDueToTransactionType`.
- Grants come only from `GRANT` and `REVOKE`: the system database runs no statement that writes graph data (`CREATE`, `MERGE`, `SET`, `DELETE`, `REMOVE`, write procedures), as in Neo4j. Deleting a role removes its grants.
- The system database runs what Neo4j 5.26.30's runs: administration and `SHOW` commands, and calls of system procedures (`worksOnSystem` in `SHOW PROCEDURES`). Any other procedure, and any other statement (`RETURN 1`, `RETURN COUNT { … }`, `EXISTS { … }`, `UNWIND …`, `CALL { … }`), fails there with `Neo.ClientError.Statement.SemanticError`, and so does a procedure call whose `YIELD`, `WHERE` or `RETURN` reads the graph (a `COUNT`, `EXISTS` or `COLLECT` subquery, a pattern comprehension or a pattern expression). A `SHOW` command's tail filters, orders, pages and projects the command's columns only: another clause after `YIELD` (`WITH`, `UNWIND`, `CALL { … }`, `MATCH`, a write) and an expression that reads the graph fail with Neo4j's `Neo.ClientError.Statement.SyntaxError` ("The COUNT expression is not valid on SHOW commands.", "Pattern expressions are not valid on SHOW commands.", "Invalid input 'WITH' …"). All of this holds on every route (directly, after `USE system`, in a statement `apoc.cypher.run` or a `CALL { }` runs, and through the embedded executor: `DB.GetCypherExecutor`, `DB.ExecuteCypher`, `DB.Cypher`). On a user database a `SHOW` tail may read the graph, as in Neo4j. A system procedure that reads the graph, such as `db.labels()`, sees none of the records the system database keeps. `EXPLAIN` of an administration command runs nothing, and `PROFILE` runs it, on both parsers.
- A custom procedure (`apoc.custom.installProcedure` / `declareProcedure`, `CREATE PROCEDURE`) is one for the whole server, whichever database it was installed from. Installing, replacing or dropping one (`apoc.custom.dropProcedure`, `DROP PROCEDURE`) needs the `admin` entitlement, as APOC 5 marks `installProcedure` and `dropProcedure` admin-only; `SHOW PROCEDURES` lists them with `admin` true. A call of a custom procedure is never answered from the result cache, so a call after a replacement runs the new body.
- A custom procedure's body runs like a statement `apoc.cypher.run` runs, with the caller's permissions: an administration command in it fails with `Neo.ClientError.Transaction.ForbiddenDueToTransactionType`, whoever calls the procedure.
- A role's name has one form everywhere: `ROLE_ops`, `Ops` and `ops` name the role `ops`.
- "Admin" means the `admin` entitlement, whatever the role holding it is called: `dbms.listConnections` lists every connection only to a principal with it (any other sees its own, as in Neo4j), and creating an API token needs it.
- A statement that selects a database with `USE`, at the top level, in `CALL { }` or in a statement a procedure runs, goes through one router: in a transaction it must stay on the transaction's database (`Neo.ClientError.Statement.AccessMode` otherwise), and on the system database the rules above apply.
- Roles are administered in Cypher on the system database: `CREATE [OR REPLACE] ROLE name [IF NOT EXISTS]`, `DROP ROLE name [IF EXISTS]`, `RENAME ROLE name TO other`, `GRANT ROLE[S] r[, …] TO user[, …]`, `REVOKE ROLE[S] r[, …] FROM user[, …]` and `SHOW [ALL | POPULATED] ROLE[S] [WITH USER[S]]`. They need administrator rights, run outside explicit transactions, and go through the same role owner as the `/auth/roles` API. `GRANT ROLE` / `REVOKE ROLE` change each named user under one lock and transaction, so concurrent commands keep each other's changes. `IMMUTABLE` roles and `CREATE ROLE … AS COPY OF` aren't supported.
- `GRANT IMMUTABLE` / `REVOKE IMMUTABLE` follow Neo4j: immutable privileges can be granted and revoked only while authentication is disabled; with it enabled, `GRANT IMMUTABLE` and any `REVOKE` of an immutable privilege fail with `Neo.ClientError.Security.Forbidden`. `REVOKE IMMUTABLE` removes only an immutable privilege; a plain `REVOKE` removes either. `SHOW PRIVILEGES` reports it in the `immutable` column.
- A procedure name or glob may be backticked, whole or in parts (`` `db.cdc.*` ``, ``db.`cdc`.*``); it is stored unquoted.
- `SHOW PRIVILEGES` (and `SHOW ROLE r PRIVILEGES`, `SHOW USER [name] PRIVILEGES`) lists the procedure, impersonation and graph privileges in Neo4j's columns (`access`, `action`, `resource`, `graph`, `segment`, `role`, `immutable`, plus `user` for a user) and takes `YIELD` and `WHERE`. A user may list their own privileges; another user's need administrator rights. Graph privileges are `GRANT` / `DENY` / `REVOKE MATCH {*} ON GRAPH g [NODES L | FOR (n:L) WHERE …]`, including collection rules with an access scope (see [Collection access](../user-guides/collection-access.md)). `DENY` of other privileges, `TRAVERSE`, `READ` and relationship segments aren't supported.

A change-feed reader, set up once:

1. Create the role (`CREATE ROLE cdc_reader`), give it the `read` entitlement (`PUT /auth/role-entitlements`) and read access on its database only (`PUT /auth/access/privileges`), and assign it to the worker login (`GRANT ROLE cdc_reader TO worker`).
2. Run `GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO cdc_reader` on the system database as an administrator.

### User Impersonation

A service that connects with one login on behalf of many users can run each user's transactions as that user, with Neo4j's `IMPERSONATE` privilege and Bolt's `imp_user`:

```cypher
// On the system database, as an administrator
GRANT IMPERSONATE (alice, bob) ON DBMS TO app_service
GRANT IMPERSONATE (*) ON DBMS TO app_service      // every user; also: GRANT IMPERSONATE ON DBMS TO …
DENY IMPERSONATE (admin) ON DBMS TO app_service
REVOKE [GRANT | DENY] IMPERSONATE (alice) ON DBMS FROM app_service
SHOW ROLE app_service PRIVILEGES                  // action impersonate, segment USER(alice) / USER(*)
```

Only a bare `*` means every user. A backticked `` `*` `` names the user called `*` (listed as ``USER(`*`)``), and a list mixing `*` with users is a syntax error.

- A Bolt 4.4+ client names the user in `imp_user` on `BEGIN`, on an auto-commit `RUN`, or on `ROUTE` (drivers: the session's `impersonated_user` / `impersonatedUser`). `null` means no impersonation; naming the signed-in user is no impersonation either. A `RUN` inside an explicit transaction runs as its `BEGIN` did.
- A `ROUTE` with `imp_user` is decided on the login's current roles, like a `RUN`, and its database (`db`, or the default database, as NornicDB has no per-user home database) must be one the impersonated user may use; the routing table names that database.
- The transaction runs in the impersonated user's complete security context: their current roles, entitlements, database allowlist, per-database privileges and procedure grants, through the same resolver as their own login. The signed-in user's privileges don't apply, not even where they are wider.
- A login may impersonate a user when one of its roles is granted that user or `*`, or it holds the `admin` entitlement (Neo4j's admin role has `ALL DBMS PRIVILEGES`, which include `IMPERSONATE`), and none of its roles is denied that user or `*`. A `DENY` wins over every grant.
- Without the privilege, the request fails with `Neo.ClientError.Security.Forbidden` before anything runs. A user who doesn't exist or is disabled is refused the same way, so a request can't learn which users exist. With authentication disabled, `imp_user` fails with `Neo.ClientError.Statement.ArgumentError`, as in Neo4j.
- While impersonating, administration commands that change the DBMS (`GRANT`, `DENY`, `REVOKE`, database and alias DDL, procedure and knowledge-policy DDL) fail with `Neo.ClientError.Security.Forbidden`, whatever the impersonated user may do, and so does a procedure that changes state every session shares (`apoc.custom.installProcedure`, `declareProcedure` and `dropProcedure`, `db.clearQueryCaches`, `db.stats.collect`, `stop` and `clear`, and user-defined or plugin procedures of mode `ADMIN`), called directly or through dynamic Cypher; `SHOW` commands run.
- A user can't be impersonated on a remote database, as Neo4j documents for remote database aliases: an impersonating `RUN`, `BEGIN`, `ROUTE` or `USE` of a remote constituent, or of a composite with one, fails with `Neo.ClientError.Security.Forbidden` before anything reaches the remote server, and the login's credential is never forwarded while impersonating.
- `TERMINATE TRANSACTIONS` is decided per transaction, as in Neo4j, for every login and for an impersonated user alike: a user may end its own transactions (while impersonating, the impersonated user's), and another user's only with the `admin` permission. A transaction it may not end is reported `Transaction not found.` and keeps running.
- Both users are kept: `SHOW TRANSACTIONS` retains Neo4j's executing `username` and default columns, and additionally supports `YIELD authenticatedUser, executingUser` (NornicDB attribution columns). `SHOW CURRENT USER` shows the executing user. The Bolt query log records `user` (the signed-in user) and, while impersonating, `authenticatedUser` and `executingUser`; change events carry `metadata.executingUser` and `metadata.authenticatedUser`.
- `GRANT` and `DENY` need the role to exist; `REVOKE` from a missing role changes nothing; immutable privileges, explicit transactions and role rename or delete behave as for procedure privileges above.
- Impersonation is a Bolt feature. NornicDB's HTTP API is Neo4j's transactional endpoint (`/db/{name}/tx`), which has no impersonation; Neo4j's Query API (`/db/{name}/query/v2`, `impersonatedUser`) isn't implemented.

### How Access Is Resolved

When a user queries a database, NornicDB resolves access in this order:

1. **Allowlist check** — Can this user's role(s) see and access this database?
2. **Global entitlements** — The roles' global entitlements (stored overrides, else the built-in defaults) cap graph-data permissions: without global `read` there is no database read/write entitlement; without `write` there is no database write entitlement. Selection access remains a separate decision.
3. **Privileges check** — Is there a (role, database) privilege entry? If yes, it decides read and write within the entitlements.
4. **Global fallback** — If no privilege entry, the global entitlements decide (admin/editor = read+write, viewer = read-only).

HTTP, Bolt, gRPC, GraphQL, MCP and plugins resolve access through this one resolver. Each operation declares its actual effects and is admitted before its database storage or services are resolved. A change to a role's entitlements or privileges applies at once on every transport. A statement that only writes (`CREATE`, and `SET` or `DELETE` of what it created) needs write but not read on the database, as in Neo4j, where a principal with `WRITE` but no `READ` can run `CREATE`; a statement that reads the graph (`MATCH`, `MERGE`, graph-reading procedures/functions, a `COUNT { }` or `EXISTS { }` subquery, a pattern comprehension) needs read, since NornicDB doesn't hide individual records from a read. Value-only expressions such as `RETURN 1` and terminating one's own transaction require selection access without a generic graph-read permission.

GDS projections belong to the executing user in the selected database. Users may create, list, use and drop their own projections with the operation's read/write permissions; selecting another owner's projection for an administrative drop requires the executing user's admin permission. Impersonating a user selects that user's catalog.

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

A principal's roles are read from the user store on every request, by one owner: for a Bearer token and for Basic credentials (whose password check the server caches for 30 seconds, but not the roles; a changed password stops working at once, on HTTP, Bolt and gRPC) on HTTP and gRPC, and for a Bolt session before every statement and transaction. A role removed from a user, or deleted (which removes it from every user), stops applying at once, as Neo4j applies role changes at once. A deleted or disabled account, or another account created later under the same name, authorizes nothing: its token is refused (HTTP 401) and its open Bolt session runs no statement. A cluster node's token (from `/api/v1/auth/cluster-token`) is not a user account and keeps its own role.

### Role Records

A role's entitlement overrides, database allowlist and database privileges are written only for a role that exists: a write for a missing role fails (HTTP 400, `Neo.ClientError.General.SchemaViolation`) and changes nothing. Each request is one transaction, so a batch (`mappings`, the privilege matrix) applies all of its entries or none, and a write racing the role's deletion or rename can't commit alongside it.

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
