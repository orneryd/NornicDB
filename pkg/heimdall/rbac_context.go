// Package heimdall provides RBAC context helpers for Bifrost request context.
// The server attaches RBAC via auth.WithRequest*; heimdall reads via these helpers
// which delegate to auth.Request*FromContext so the same context works for Bifrost and GraphQL.
package heimdall

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/auth"
)

// PrincipalRolesFromContext returns the principal's roles from the request context, or nil.
func PrincipalRolesFromContext(ctx context.Context) []string {
	return auth.RequestPrincipalRolesFromContext(ctx)
}

// DatabaseAccessModeFromContext returns the principal's DatabaseAccessMode from context, or nil.
func DatabaseAccessModeFromContext(ctx context.Context) auth.DatabaseAccessMode {
	return auth.RequestDatabaseAccessModeFromContext(ctx)
}

// ResolvedAccessResolverFromContext returns the ResolvedAccess resolver from context, or nil.
func ResolvedAccessResolverFromContext(ctx context.Context) func(string) auth.ResolvedAccess {
	return auth.RequestResolvedAccessResolverFromContext(ctx)
}
