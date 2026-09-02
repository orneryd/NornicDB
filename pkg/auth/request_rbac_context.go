// Package auth provides request-scoped RBAC context helpers.
// When the server mounts authenticated endpoints (Bifrost, GraphQL), it can
// attach principal roles, DatabaseAccessMode, and ResolvedAccess resolver to
// the request context so handlers and resolvers can enforce per-database access.
package auth

import (
	"context"
	"strings"
)

type requestRBACKey string

var (
	requestRBACKeyPrincipalRoles         = requestRBACKey("principal_roles")
	requestRBACKeyDatabaseAccessMode     = requestRBACKey("database_access_mode")
	requestRBACKeyResolvedAccessResolver = requestRBACKey("resolved_access_resolver")
	requestRBACKeyDatabaseScope          = requestRBACKey("database_scope")
)

// RequestDatabaseScope is the server-derived set of database names and aliases
// an authenticated principal may select for one request.
type RequestDatabaseScope struct {
	defaultDatabase string
	selections      map[string]string
	canonical       []string
}

// NewRequestDatabaseScope constructs an immutable request scope. Selection
// keys are normalized for lookup and the supplied map is defensively copied.
func NewRequestDatabaseScope(defaultDatabase string, selections map[string]string) *RequestDatabaseScope {
	scope := &RequestDatabaseScope{selections: make(map[string]string, len(selections))}
	canonicalSeen := make(map[string]struct{})
	for selection, database := range selections {
		selection = normalizeDatabaseSelection(selection)
		database = strings.TrimSpace(database)
		if selection == "" || database == "" {
			continue
		}
		scope.selections[selection] = database
		if _, exists := canonicalSeen[database]; !exists {
			canonicalSeen[database] = struct{}{}
			scope.canonical = append(scope.canonical, database)
		}
	}
	if database, ok := scope.selections[normalizeDatabaseSelection(defaultDatabase)]; ok {
		scope.defaultDatabase = database
	}
	return scope
}

// Resolve returns the canonical database for an explicit selection. An omitted
// selection resolves to the authorized default, or to the sole authorized
// database when no authorized default exists.
func (s *RequestDatabaseScope) Resolve(selection string) (string, bool) {
	if s == nil {
		return "", false
	}
	selection = normalizeDatabaseSelection(selection)
	if selection != "" {
		database, ok := s.selections[selection]
		return database, ok
	}
	if s.defaultDatabase != "" {
		return s.defaultDatabase, true
	}
	if len(s.canonical) == 1 {
		return s.canonical[0], true
	}
	return "", false
}

func normalizeDatabaseSelection(selection string) string {
	return strings.ToLower(strings.TrimSpace(selection))
}

// WithRequestPrincipalRoles attaches the principal's role names to the context.
func WithRequestPrincipalRoles(ctx context.Context, roles []string) context.Context {
	return context.WithValue(ctx, requestRBACKeyPrincipalRoles, roles)
}

// WithRequestDatabaseAccessMode attaches the principal's per-database access mode to the context.
func WithRequestDatabaseAccessMode(ctx context.Context, mode DatabaseAccessMode) context.Context {
	return context.WithValue(ctx, requestRBACKeyDatabaseAccessMode, mode)
}

// WithRequestResolvedAccessResolver attaches a resolver (dbName -> ResolvedAccess) to the context.
func WithRequestResolvedAccessResolver(ctx context.Context, fn func(string) ResolvedAccess) context.Context {
	return context.WithValue(ctx, requestRBACKeyResolvedAccessResolver, fn)
}

// WithRequestDatabaseScope attaches the server-derived database scope.
func WithRequestDatabaseScope(ctx context.Context, scope *RequestDatabaseScope) context.Context {
	return context.WithValue(ctx, requestRBACKeyDatabaseScope, scope)
}

// RequestPrincipalRolesFromContext returns the principal's roles from the request context, or nil.
func RequestPrincipalRolesFromContext(ctx context.Context) []string {
	v, _ := ctx.Value(requestRBACKeyPrincipalRoles).([]string)
	return v
}

// RequestDatabaseAccessModeFromContext returns the principal's DatabaseAccessMode from context, or nil.
func RequestDatabaseAccessModeFromContext(ctx context.Context) DatabaseAccessMode {
	v, _ := ctx.Value(requestRBACKeyDatabaseAccessMode).(DatabaseAccessMode)
	return v
}

// RequestResolvedAccessResolverFromContext returns the ResolvedAccess resolver from context, or nil.
func RequestResolvedAccessResolverFromContext(ctx context.Context) func(string) ResolvedAccess {
	v, _ := ctx.Value(requestRBACKeyResolvedAccessResolver).(func(string) ResolvedAccess)
	return v
}

// RequestDatabaseScopeFromContext returns the server-derived database scope.
func RequestDatabaseScopeFromContext(ctx context.Context) *RequestDatabaseScope {
	v, _ := ctx.Value(requestRBACKeyDatabaseScope).(*RequestDatabaseScope)
	return v
}
