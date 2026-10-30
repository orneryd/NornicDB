// Package auth: canonical enumeration of RBAC entitlements.
//
// Entitlements are the individual permissions/rights that can be assigned to roles.
// They include global permissions (read, write, admin, etc.) and per-database
// entitlements (see database, access database, read, write). Use this list for
// UI, APIs, and documentation so admins can assign and audit entitlements per role.
package auth

// EntitlementCategory indicates whether an entitlement is global or per-database.
type EntitlementCategory string

const (
	EntitlementCategoryGlobal      EntitlementCategory = "global"
	EntitlementCategoryPerDatabase EntitlementCategory = "per_database"
)

// Entitlement describes a single assignable permission or right.
type Entitlement struct {
	ID          string              `json:"id"`          // Stable ID (e.g. "read", "database_write")
	Name        string              `json:"name"`        // Display name
	Description string              `json:"description"` // What this entitlement gates
	Category    EntitlementCategory `json:"category"`    // global or per_database
}

// AllEntitlements returns the full list of entitlements with descriptions.
// Use this for UI (assign entitlements to roles), APIs (GET /auth/entitlements),
// and documentation. Global entitlements map to Permission; per-database
// entitlements map to allowlist (see/access) and privileges matrix (read/write).
func AllEntitlements() []Entitlement {
	return []Entitlement{
		// --- Global entitlements (Permission) ---
		{
			ID:          string(PermRead),
			Name:        "Read",
			Description: "Read data: Cypher MATCH, HTTP/status/metrics, auth/me, search, Bifrost/GraphQL read, MCP read. Required for per-database read when privileges matrix is used.",
			Category:    EntitlementCategoryGlobal,
		},
		{
			ID:          string(PermWrite),
			Name:        "Write",
			Description: "Write data: Cypher CREATE/DELETE/SET/MERGE (when ResolvedAccess.Write for that DB), embed/trigger, search/rebuild, Bifrost/GraphQL mutations, MCP write.",
			Category:    EntitlementCategoryGlobal,
		},
		{
			ID:          string(PermCreate),
			Name:        "Create",
			Description: "Create operations (e.g. GDPR delete requires delete; create is used for resource creation where distinguished from write).",
			Category:    EntitlementCategoryGlobal,
		},
		{
			ID:          string(PermDelete),
			Name:        "Delete",
			Description: "Delete operations: e.g. GDPR delete (/gdpr/delete).",
			Category:    EntitlementCategoryGlobal,
		},
		{
			ID:          string(PermAdmin),
			Name:        "Admin",
			Description: "Admin-only: API token generation, roles CRUD, database access/privileges APIs, embed/clear, /admin/* (stats, config, backup, GPU), Snapshots (Qdrant). Implies full system control.",
			Category:    EntitlementCategoryGlobal,
		},
		{
			ID:          string(PermSchema),
			Name:        "Schema",
			Description: "Schema operations: Bolt INDEX/CONSTRAINT Cypher. Required for creating/dropping indexes and constraints.",
			Category:    EntitlementCategoryGlobal,
		},
		{
			ID:          string(PermUserManage),
			Name:        "User management",
			Description: "User management: /auth/users and /auth/users/:id (list, create, update, delete users). Distinct from admin (roles/DB access).",
			Category:    EntitlementCategoryGlobal,
		},
		// --- Per-database entitlements ---
		{
			ID:          "database_see",
			Name:        "Database: see",
			Description: "Per-database: Database appears in SHOW DATABASES and catalogue. Granted via allowlist (role → list of databases). Empty list = all databases.",
			Category:    EntitlementCategoryPerDatabase,
		},
		{
			ID:          "database_access",
			Name:        "Database: access",
			Description: "Per-database: Principal may run Cypher/GraphQL/Bolt against this database (subject to read/write). Granted via allowlist.",
			Category:    EntitlementCategoryPerDatabase,
		},
		{
			ID:          "database_read",
			Name:        "Database: read",
			Description: "Per-database: Read operations (MATCH, read properties) on this database. Set via /auth/access/privileges or derived from role (viewer=read-only, editor/admin=read+write).",
			Category:    EntitlementCategoryPerDatabase,
		},
		{
			ID:          "database_write",
			Name:        "Database: write",
			Description: "Per-database: Write operations (CREATE, DELETE, SET, MERGE) on this database. Set via /auth/access/privileges or derived from role.",
			Category:    EntitlementCategoryPerDatabase,
		},
	}
}

// GlobalEntitlementIDs returns the IDs of all global entitlements (same as Permission values for read/write/create/delete/admin/schema/user_manage).
func GlobalEntitlementIDs() []string {
	return []string{
		string(PermRead), string(PermWrite), string(PermCreate), string(PermDelete),
		string(PermAdmin), string(PermSchema), string(PermUserManage),
	}
}

// PerDatabaseEntitlementIDs returns the IDs of per-database entitlements.
func PerDatabaseEntitlementIDs() []string {
	return []string{"database_see", "database_access", "database_read", "database_write"}
}

// RolePermissionsAsStrings returns role → permission IDs for fallback when role entitlements store has no override.
// Used by Bolt and other consumers that need the default role→permissions mapping. IDs match GlobalEntitlementIDs().
func RolePermissionsAsStrings() map[string][]string {
	m := make(map[string][]string, len(RolePermissions))
	for role, perms := range RolePermissions {
		ids := make([]string, len(perms))
		for i, p := range perms {
			ids[i] = string(p)
		}
		m[string(role)] = ids
	}
	return m
}
