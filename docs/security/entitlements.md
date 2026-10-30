# RBAC Entitlements Reference

**Canonical list of entitlements (permissions) that can be assigned to roles.**

NornicDB RBAC uses two kinds of entitlements:

1. **Global entitlements** – Apply across the server (e.g. read, write, admin). These map to the `Permission` type and are checked on HTTP/Bolt routes and in code.
2. **Per-database entitlements** – Apply per database (see/access a DB, read/write on that DB). These are enforced via the allowlist and privileges matrix.

The server exposes the full list at **GET /auth/entitlements** (requires read permission). Use it for UI (assign entitlements to roles) and for auditing.

---

## Global entitlements

| ID | Name | What it gates |
|----|------|----------------|
| **read** | Read | Read data: Cypher MATCH, `/db/*`, `/status`, `/metrics`, `/auth/me`, `/auth/password`, `/auth/profile`, `/nornicdb/search`, `/nornicdb/similar`, `/nornicdb/decay`, `/nornicdb/embed/stats`, `/nornicdb/embed/failures`, Bifrost/GraphQL read, MCP read, `/gdpr/export`. Required for per-database read when privileges matrix is used. |
| **write** | Write | Write data: Cypher CREATE/DELETE/SET/MERGE (when ResolvedAccess.Write for that DB), `/nornicdb/embed/trigger`, `/nornicdb/embed/retry-failures`, `/nornicdb/search/rebuild`, Bifrost/GraphQL mutations, MCP write. |
| **create** | Create | Create operations (resource creation where distinguished from write). |
| **delete** | Delete | Delete operations: e.g. `/gdpr/delete`. |
| **admin** | Admin | Admin-only: `/auth/api-token`, `/auth/roles`, `/auth/roles/*`, `/auth/access/databases`, `/auth/access/privileges`, `/auth/entitlements` (read-only list), `/nornicdb/embed/clear`, `/admin/*` (stats, config, backup, GPU), Qdrant snapshots. Implies full system control. |
| **schema** | Schema | Schema operations: Bolt INDEX/CONSTRAINT Cypher. Required for creating/dropping indexes and constraints. |
| **user_manage** | User management | User management: `/auth/users`, `/auth/users/*` (list, create, update, delete users). Distinct from admin (roles/DB access). |

### Role defaults (built-in)

- **admin**: read, write, create, delete, admin, schema, user_manage  
- **editor**: read, write, create, delete  
- **viewer**: read  

User-defined roles have no global entitlements by default; assign them via role configuration or (when implemented) per-role entitlement assignment.

---

## Per-database entitlements

These apply **per database** and are configured via:

- **Allowlist** (`/auth/access/databases`): which roles can **see** and **access** which databases.  
- **Privileges matrix** (`/auth/access/privileges`): per (role, database) **read** and **write**.

| ID | Name | What it gates |
|----|------|----------------|
| **database_see** | Database: see | Database appears in SHOW DATABASES and catalogue. Granted via allowlist (role → list of databases). Empty list = all databases. |
| **database_access** | Database: access | Principal may run Cypher/GraphQL/Bolt against this database (subject to read/write). Granted via allowlist. |
| **database_read** | Database: read | Read operations (MATCH, read properties) on this database. Set via `/auth/access/privileges` or derived from role (viewer = read-only, editor/admin = read+write). |
| **database_write** | Database: write | Write operations (CREATE, DELETE, SET, MERGE) on this database. Set via `/auth/access/privileges` or derived from role. |

---

## API

- **GET /auth/entitlements** – Returns the full list of entitlements (id, name, description, category). Requires read permission.

---

## See also

- [Per-Database RBAC & Lockout Recovery](per-database-rbac.md) – Allowlist, privileges, and recovery
- [Design: Per-Database RBAC](per-database-rbac.md) – Design and alignment with Neo4j
