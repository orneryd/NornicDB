package auth

// CatalogVisibility is the principal's live metadata view. It binds permission
// metadata rather than an execution context, so it can travel with row values
// without acquiring cancellation ownership. Its zero value is trusted embedded
// use with no configured authorization policy.
type CatalogVisibility struct {
	mode      DatabaseAccessMode
	scope     *RequestDatabaseScope
	selection func(database, permission string) bool
}

// NewCatalogVisibility binds the existing visibility mode, request selection
// scope and permission resolver. No decision is cached: retained store-backed
// modes and resolvers remain live. For example, bind once for a statement row,
// then call view.CanSeeDatabase("reports") before publishing catalog metadata.
// A nil component leaves that component unconfigured.
func NewCatalogVisibility(mode DatabaseAccessMode, scope *RequestDatabaseScope, selection func(database, permission string) bool) CatalogVisibility {
	return CatalogVisibility{mode: mode, scope: scope, selection: selection}
}

// CanSeeDatabase reports whether database may be named in catalog output.
// Explicit metadata visibility does not require data-read privileges. A request
// scope remains a selection boundary; absent an explicit mode, the existing
// access resolver decides, preserving trusted callers without a resolver.
func (v CatalogVisibility) CanSeeDatabase(database string) bool {
	if v.mode != nil && !v.mode.CanSeeDatabase(database) {
		return false
	}
	if v.scope != nil {
		if _, allowed := v.scope.Resolve(database); !allowed {
			return false
		}
	}
	return v.mode != nil || v.selection == nil || v.selection(database, "access")
}
