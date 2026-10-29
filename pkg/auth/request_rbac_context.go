// Package auth provides request-scoped RBAC context helpers.
// When the server mounts authenticated endpoints (Bifrost, GraphQL), it can
// attach principal roles, DatabaseAccessMode, and ResolvedAccess resolver to
// the request context so handlers and resolvers can enforce per-database access.
package auth

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

type requestRBACKey uint8

const (
	requestRBACKeyResolvedAccessResolver requestRBACKey = iota
	requestRBACKeyPrincipalRoles
	requestRBACKeyDatabaseAccessMode
	requestRBACKeyEntitlementResolver
	requestRBACKeyDatabaseScope
	requestRBACKeyAuthenticationDisabled
)

// RequestEntitlementResolver checks an effective permission for a database.
type RequestEntitlementResolver func(database string, permission Permission) bool

// RequestDatabaseScope is the server-derived set of database names and aliases
// available to a request. A live selection check can restrict these catalog names.
type RequestDatabaseScope struct {
	defaultDatabase string
	selections      map[string]string
	canonical       []string
	selectionCheck  func(string) bool
}

// NewRequestDatabaseScope constructs an immutable request scope. Selection
// keys are normalized for lookup and the supplied map is defensively copied.
func NewRequestDatabaseScope(defaultDatabase string, selections map[string]string) *RequestDatabaseScope {
	return NewRequestDatabaseScopeWithSelectionCheck(defaultDatabase, selections, nil)
}

// NewRequestDatabaseScopeWithSelectionCheck binds the existing live graph-selection
// owner. Catalog aliases do not confer authority, and later grants/revocations
// are observed without rebuilding the request context.
func NewRequestDatabaseScopeWithSelectionCheck(defaultDatabase string, selections map[string]string, check func(string) bool) *RequestDatabaseScope {
	scope := &RequestDatabaseScope{selections: make(map[string]string, len(selections)), selectionCheck: check}
	canonicalSeen := make(map[string]struct{})
	for selection, database := range selections {
		selection = normalizeDatabaseSelection(selection)
		if selection == "" || database == "" {
			continue
		}
		scope.selections[selection] = database
		if _, exists := canonicalSeen[database]; !exists {
			canonicalSeen[database] = struct{}{}
			scope.canonical = append(scope.canonical, database)
		}
	}
	if _, ok := scope.selections[normalizeDatabaseSelection(defaultDatabase)]; ok {
		scope.defaultDatabase = normalizeDatabaseSelection(defaultDatabase)
	}
	return scope
}

// Resolve validates the requested name before returning its canonical database.
// An omitted name uses the configured default or the sole scoped database.
func (s *RequestDatabaseScope) Resolve(selection string) (string, bool) {
	database, found := s.resolve(selection)
	return database, found && s.allowsSelection(selection, database)
}

func (s *RequestDatabaseScope) allowsSelection(selection, database string) bool {
	if s.selectionCheck == nil {
		return true
	}
	selection = normalizeDatabaseSelection(selection)
	if selection == "" {
		selection = s.defaultDatabase
		if selection == "" {
			selection = database
		}
	}
	return s.selectionCheck(selection)
}

// resolve binds catalog identity without caching an authorization decision.
func (s *RequestDatabaseScope) resolve(selection string) (string, bool) {
	if s == nil {
		return "", false
	}
	selection = normalizeDatabaseSelection(selection)
	if selection != "" {
		database, ok := s.selections[selection]
		return database, ok
	}
	if s.defaultDatabase != "" {
		return s.selections[s.defaultDatabase], true
	}
	if len(s.canonical) == 1 {
		return s.canonical[0], true
	}
	return "", false
}

func normalizeDatabaseSelection(selection string) string {
	return cyphertext.NameLower(strings.TrimSpace(selection))
}

// WithRequestPrincipalRoles attaches the principal's role names to the context.
func WithRequestPrincipalRoles(ctx context.Context, roles []string) context.Context {
	return context.WithValue(ctx, requestRBACKeyPrincipalRoles, roles)
}

// WithRequestDatabaseAccessMode attaches the principal's per-database access mode to the context.
func WithRequestDatabaseAccessMode(ctx context.Context, mode DatabaseAccessMode) context.Context {
	return context.WithValue(ctx, requestRBACKeyDatabaseAccessMode, mode)
}

// WithRequestResolvedAccessResolver preserves the legacy read/write context
// adapter. For example, attach a function returning ResolvedAccess{Read: true}
// to supply a legacy read-only consumer. It leaves any complete entitlement
// resolver unchanged, including administrator and schema checks; a nil fn clears
// the legacy adapter without clearing those checks.
//
// Deprecated: production transports should attach the complete permission owner
// with WithRequestEntitlementResolver instead.
func WithRequestResolvedAccessResolver(ctx context.Context, fn func(string) ResolvedAccess) context.Context {
	return context.WithValue(ctx, requestRBACKeyResolvedAccessResolver, fn)
}

// WithRequestEntitlementResolver attaches the principal's one permission resolver
// (the server builds it from cypher.PrincipalAccess): whether it holds a
// permission on a database. RequestResolvedAccessResolverFromContext reads
// read and write from it.
func WithRequestEntitlementResolver(ctx context.Context, resolver RequestEntitlementResolver) context.Context {
	return context.WithValue(ctx, requestRBACKeyEntitlementResolver, resolver)
}

// WithRequestPrincipal attaches a principal as callers that authorize through
// the request read it: its permission resolver (WithRequestEntitlementResolver)
// and the databases and aliases it may select (WithRequestDatabaseScope),
// together. A context with a resolver and no scope is an incomplete
// principal, which those callers refuse.
func WithRequestPrincipal(ctx context.Context, resolver RequestEntitlementResolver, scope *RequestDatabaseScope) context.Context {
	return WithRequestDatabaseScope(WithRequestEntitlementResolver(ctx, resolver), scope)
}

// WithRequestAuthenticationDisabled marks a request the server serves with
// authentication disabled, which therefore carries no principal. Callers that
// authorize through the request principal (RequestEntitlementResolverFromContext)
// treat a context with neither a resolver nor this mark as refused.
func WithRequestAuthenticationDisabled(ctx context.Context) context.Context {
	return context.WithValue(ctx, requestRBACKeyAuthenticationDisabled, true)
}

// RequestAuthenticationDisabledFromContext reports whether the server marked
// the request as served with authentication disabled
// (WithRequestAuthenticationDisabled).
func RequestAuthenticationDisabledFromContext(ctx context.Context) bool {
	disabled, _ := ctx.Value(requestRBACKeyAuthenticationDisabled).(bool)
	return disabled
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

// RequestResolvedAccessResolverFromContext returns the principal's read and
// write on a database (dbName -> ResolvedAccess), read from the one
// entitlement resolver the context carries (WithRequestEntitlementResolver),
// or nil when it carries none. An explicitly attached legacy adapter takes
// precedence for this read/write compatibility accessor only.
func RequestResolvedAccessResolverFromContext(ctx context.Context) func(string) ResolvedAccess {
	if legacy, attached := ctx.Value(requestRBACKeyResolvedAccessResolver).(func(string) ResolvedAccess); attached {
		return legacy
	}
	allows := RequestEntitlementResolverFromContext(ctx)
	if allows == nil {
		return nil
	}
	return func(database string) ResolvedAccess {
		return ResolvedAccess{Read: allows(database, PermRead), Write: allows(database, PermWrite)}
	}
}

// RequestEntitlementResolverFromContext returns the effective entitlement resolver.
func RequestEntitlementResolverFromContext(ctx context.Context) RequestEntitlementResolver {
	resolver, _ := ctx.Value(requestRBACKeyEntitlementResolver).(RequestEntitlementResolver)
	return resolver
}

// RequestDatabaseScopeFromContext returns the server-derived database scope.
func RequestDatabaseScopeFromContext(ctx context.Context) *RequestDatabaseScope {
	v, _ := ctx.Value(requestRBACKeyDatabaseScope).(*RequestDatabaseScope)
	return v
}
