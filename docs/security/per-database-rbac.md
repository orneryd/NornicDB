# Per-Database RBAC and Lockout Recovery

**Per-database access control and how to recover from admin lockout.**

This document describes NornicDB’s per-database role-based access (allowlist and per-DB read/write) and **actions that can lock you out** and **how to fix them**. It is intended for operators and admins who manage Database Access (allowlist and privileges) in the Security / Admin area.

---

## Overview

When authentication is enabled, NornicDB enforces:

1. **Database allowlist** – Which roles can **see** and **access** which databases (HTTP path, Bolt database, Bifrost/Heimdall).
2. **Per-database privileges** – Per (role, database) **read** and **write** (mutations: CREATE, DELETE, SET, MERGE, etc.).

For a full list of entitlements (global permissions and per-database rights) and what each gates, see **[RBAC Entitlements Reference](entitlements.md)**. The server also exposes **GET /auth/entitlements** for UI and tooling.

All of this is stored in the **system database** and loaded at startup. The built-in roles **admin**, **editor**, and **viewer** are always present; they can have allowlist and privilege entries like user-defined roles. If no allowlist is configured for a role, that role is treated as having access to **all databases** (empty list = all). If no per-DB privilege entry exists for a (role, database), resolution falls back to global role permissions (e.g. editor = read+write, viewer = read-only).

When a new database is created (via **CREATE DATABASE** or **CREATE COMPOSITE DATABASE** Cypher), NornicDB automatically grants full access (see, access, read, write) to that database for: (1) the **admin** role, and (2) every role held by the principal who executed the command. So admins always retain access to new databases, and the creator (user or service principal) gets full permissions on the database they created.

- **DatabaseAccessMode not set = no access**  
  When authorization is enabled and no allowlist/roles are configured for a principal, the effective mode is **deny all**: the principal cannot see or access any database until an operator configures the allowlist (and optionally per-DB privileges). This matches Neo4j: explicit **GRANT** or roles are required.

---

## Actions That Can Lock You Out

Because admins can edit **any** role (including **admin**), the following can remove your ability to manage the system:

1. **Removing admin from database access**  
   Clearing the **admin** role’s allowlist or restricting it to **no databases** means no one with only the admin role can access any database (including the system DB if it’s in the allowlist). You cannot open the Database Access or Admin UI for that database.

2. **Revoking the admin role from the only admin user(s)**  
   If the only user(s) with the **admin** role have that role removed, no one can reach the admin UI or RBAC APIs (`/auth/roles`, `/auth/access/databases`, `/auth/access/privileges`, user management). All of these require the admin role.

3. **Changing or disabling the default admin without another admin**  
   Changing the default admin’s password or disabling the account without ensuring at least one other user has the admin role can leave you with no way to log in as admin.

---

## How to Fix It (Recovery)

### 1. Re-run seed (recommended)

On server startup, if the system database has **no** RBAC configuration (no allowlist data), NornicDB runs a **seed** that:

- Pre-configures **admin**, **editor**, and **viewer** with access to **all databases** (empty allowlist = all).
- Ensures the default admin account exists (or documents that the first user created must be assigned the admin role).

To recover using seed:

- **Option A:** If your deployment supports a “re-seed” or “reset RBAC” maintenance command or flag, use it so that the allowlist (and optionally default admin) is restored. Then log in and fix roles/allowlists/privileges as needed.
- **Option B:** Restore the system database from a backup taken when RBAC was still correct, then restart the server so the restored allowlist and users are loaded.

After seed or restore, log in with an admin account and re-apply any custom allowlist or per-DB privilege settings you need.

### 2. Recovery account

Maintain a **separate recovery admin account** that is:

- Not modified or disabled via the normal Admin UI.
- Created once (e.g. via config or a one-off script) and stored in a secure place.
- Used only for emergency access to restore allowlist and admin role assignments.

Use this account to log in, restore the admin role and database access for normal admins, then avoid using the recovery account for day-to-day changes.

### 3. Direct system database fix

If the server is unreachable or you have filesystem/DB access only, you can fix data **directly in the system database**:

- **Schema (conceptual):**  
  - Allowlist: nodes with label `_RoleDbAccess` (or equivalent), e.g. node ID `role_db_access:{roleName}`, property `databases` (array of strings). Empty or missing list = all databases.  
  - Per-DB privileges: nodes with label `_DbPrivilege`, e.g. node ID `db_priv:{roleName}:{dbName}`, properties `role`, `database`, `read` (bool), `write` (bool).  
  - Users and roles: user nodes and role nodes as described in [system database docs](../user-guides/multi-database.md#system-database) and the main RBAC design.

- **Steps (high level):**  
  1. Stop the server or ensure no one else is writing to the system DB.  
  2. Open the system database with the same tooling NornicDB uses (e.g. Badger/engine).  
  3. Restore or insert allowlist entries for **admin** (e.g. empty `databases` for “all databases”).  
  4. Restore or insert per-DB privilege entries if you use them.  
  5. Ensure at least one user has the **admin** role (e.g. restore user node `roles` or fix role assignment).  
  6. Restart the server so it reloads from the system DB.

Exact property names and node IDs are defined in the codebase (`pkg/auth`: allowlist, privileges, roles). Refer to the implementation and [Per-Database RBAC & Lockout Recovery](per-database-rbac.md) for the single source of truth.

---

## Where This Is Documented in the UI

The Database Access (or Access Control) management screen in the Security / Admin area should link to this document with wording such as: **“Learn what can lock you out and how to recover.”**

---

## See Also

- [Per-database RBAC design](per-database-rbac.md) – Full design, storage layout, and API.
- [RBAC (compliance)](../compliance/rbac.md) – Roles, permissions, and user management.
- [User storage in system DB](../user-guides/multi-database.md#system-database) – Where users and system data live.
