package auth

import "context"

type readOnlyRequestMode struct{ allows func(string) bool }

func (m readOnlyRequestMode) CanSeeDatabase(database string) bool    { return m.allows(database) }
func (m readOnlyRequestMode) CanAccessDatabase(database string) bool { return m.allows(database) }

// WithReadOnlyDatabaseScope narrows the caller's live principal to one readable
// database. Identity and cancellation are preserved; legacy access adapters are
// narrowed too, so an inherited administrator adapter cannot reintroduce writes.
func WithReadOnlyDatabaseScope(ctx context.Context, selection string) context.Context {
	original := ctx
	database := ""
	originalScope := RequestDatabaseScopeFromContext(original)
	if scope := originalScope; scope != nil {
		if canonical, ok := scope.Resolve(selection); ok {
			database = canonical
		}
	}
	originalAllows := RequestEntitlementResolverFromContext(original)
	originalMode := RequestDatabaseAccessModeFromContext(original)
	read := func(target string) bool {
		if database == "" || target != database || originalAllows == nil {
			return false
		}
		if canonical, ok := originalScope.Resolve(selection); !ok || canonical != database {
			return false
		}
		return (originalMode == nil || originalMode.CanAccessDatabase(database)) && originalAllows(database, PermRead)
	}
	selections := map[string]string{}
	if database != "" {
		selections[database] = database
		selections[selection] = database
	}
	grant := func(target string, permission Permission) bool {
		return (permission == PermRead || permission == Permission("access")) && read(target)
	}
	ctx = WithRequestPrincipal(ctx, grant, NewRequestDatabaseScopeWithSelectionCheck(database, selections, func(selection string) bool { return read(database) }))
	ctx = WithRequestDatabaseAccessMode(ctx, readOnlyRequestMode{allows: read})
	return WithRequestResolvedAccessResolver(ctx, func(target string) ResolvedAccess { return ResolvedAccess{Read: read(target)} })
}
