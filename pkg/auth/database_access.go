// Package auth: per-database access types (Neo4j-aligned).
//
// PrivilegeDatabaseRef, DatabaseAccessMode, and ResolvedAccess align with
// Neo4j's kernel SecurityContext and DatabaseAccessMode so that "which
// user/role can access which database" is enforced consistently across
// HTTP, Bolt, GraphQL, and Heimdall. See docs/plans/per-database-rbac-neo4j-style.md.

package auth

import "context"

// PrivilegeDatabaseRef identifies the database for privilege checks.
// Equivalent to Neo4j PrivilegeDatabaseReference (name, owningDatabaseName).
// For normal (non-composite) databases, Name and OwningDatabaseName are the same.
type PrivilegeDatabaseRef struct {
	Name               string // Normalized database name, e.g. "nornic", "system"
	OwningDatabaseName string // For composite/sharded; empty or Name for normal DBs
}

// DatabaseAccessMode answers whether a principal may see and access databases.
// Equivalent to Neo4j DatabaseAccessMode. Used before executing Cypher:
// CanAccessDatabase(dbName) must be true or the request is denied with 403.
type DatabaseAccessMode interface {
	CanSeeDatabase(dbName string) bool
	CanAccessDatabase(dbName string) bool
}

// fullDBAccessMode allows see and access to all databases.
// Used when auth is disabled or when explicitly configured for full access.
type fullDBAccessMode struct{}

func (fullDBAccessMode) CanSeeDatabase(string) bool    { return true }
func (fullDBAccessMode) CanAccessDatabase(string) bool { return true }

// FullDatabaseAccessMode is the singleton that allows all databases.
// Use when auth is disabled so dev/local usage works without RBAC.
var FullDatabaseAccessMode DatabaseAccessMode = fullDBAccessMode{}

// denyAllDBAccessMode denies see and access to all databases.
// Used when DatabaseAccessMode is nil (secure-by-default).
type denyAllDBAccessMode struct{}

func (denyAllDBAccessMode) CanSeeDatabase(string) bool    { return false }
func (denyAllDBAccessMode) CanAccessDatabase(string) bool { return false }

// DenyAllDatabaseAccessMode is the singleton that denies all databases.
// Use when auth is enabled and no allowlist is configured (secure default).
var DenyAllDatabaseAccessMode DatabaseAccessMode = denyAllDBAccessMode{}

// ResolvedAccess is the resolved read/write capability for a (principal, database).
// Equivalent to Neo4j AccessMode for a single DB. Used for execution-time
// mutation checks: Write must be true to allow CREATE, DELETE, SET, MERGE, etc.
type ResolvedAccess struct {
	Read  bool // Allow MATCH, read properties, etc.
	Write bool // Allow CREATE, DELETE, SET, MERGE, etc.
}

// ResolveDatabaseReadAccess is the shared direct-read decision for protocol
// routes that read a database without going through Cypher. mode owns whether
// the principal may select the database; resolve owns its per-database read
// privilege. A nil mode or resolver preserves configurations where that layer
// of authorization is disabled.
func ResolveDatabaseReadAccess(mode DatabaseAccessMode, resolve func(string) ResolvedAccess, database string) (accessible, readable bool) {
	if mode != nil && !mode.CanAccessDatabase(database) {
		return false, false
	}
	if resolve == nil {
		return true, true
	}
	return true, resolve(database).Read
}

// RequestDatabasePermissionAllowed applies the existing request permission
// resolver and database selection rules to a direct (non-Cypher) operation.
// It does not introduce privileges or grant authority to acquire a database.
func RequestDatabasePermissionAllowed(ctx context.Context, database string, permission Permission) bool {
	if permission == PermRead {
		_, allows := BindRequestDatabaseRead(ctx, database)
		return allows()
	}
	if ctx == nil {
		return false
	}
	mode := RequestDatabaseAccessModeFromContext(ctx)
	if mode != nil && !mode.CanAccessDatabase(database) {
		return false
	}
	if scope := RequestDatabaseScopeFromContext(ctx); scope != nil {
		if _, allowed := scope.Resolve(database); !allowed {
			return false
		}
	}
	if allows := RequestEntitlementResolverFromContext(ctx); allows != nil {
		return allows(database, permission)
	}
	if permission == PermWrite {
		if resolve := RequestResolvedAccessResolverFromContext(ctx); resolve != nil {
			return resolve(database).Write
		}
	}
	return true
}

// BindRequestDatabaseRead resolves a request selection once and binds its live
// read checks. It caches no permission result: mode and resolver callbacks run
// on each invocation, including legacy callbacks without a change generation.
// Consumers can retain the canonical database and predicate for their lifetime.
func BindRequestDatabaseRead(ctx context.Context, database string) (string, func() bool) {
	refused := func() bool { return false }
	if ctx == nil {
		return database, refused
	}
	selection := database
	if scope := RequestDatabaseScopeFromContext(ctx); scope != nil {
		canonical, allowed := scope.Resolve(database)
		if !allowed {
			return database, refused
		}
		database = canonical
	}
	mode := RequestDatabaseAccessModeFromContext(ctx)
	legacy, _ := ctx.Value(requestRBACKeyResolvedAccessResolver).(func(string) ResolvedAccess)
	allows := RequestEntitlementResolverFromContext(ctx)
	return database, func() bool {
		if mode != nil && (!mode.CanAccessDatabase(selection) || selection != database && !mode.CanAccessDatabase(database)) {
			return false
		}
		if legacy != nil {
			return legacy(database).Read
		}
		return allows == nil || allows(database, PermRead)
	}
}
