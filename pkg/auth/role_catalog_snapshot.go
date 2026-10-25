package auth

import (
	"slices"

	"github.com/orneryd/nornicdb/internal/collection"
)

// RoleCatalogSnapshot contains independently owned role metadata. Overrides
// contain stored entitlement IDs; PermissionsForRole resolves effective grants.
type RoleCatalogSnapshot struct {
	RoleNames            []string
	EntitlementOverrides map[string][]string
}

// ReadRoleCatalog reads each supplied store under its own read lock. Pass nil
// for a view that is not needed; its result field remains nil. Role names keep
// built-ins first and sort user-defined names. Maps and slices belong to the caller.
//
// Example:
//
// catalog := auth.ReadRoleCatalog(roleStore, entitlementStore)
//
//	for _, name := range catalog.RoleNames {
//	    fmt.Println(name, catalog.EntitlementOverrides[name])
//	}
func ReadRoleCatalog(roles *RoleStore, entitlements *RoleEntitlementsStore) RoleCatalogSnapshot {
	var snapshot RoleCatalogSnapshot
	if roles != nil {
		snapshot.RoleNames = collection.ReadSnapshot(&roles.mu, func() []string {
			return slices.Concat(builtinRoleNames, collection.SortedKeys(roles.roles))
		})
	}
	if entitlements != nil {
		snapshot.EntitlementOverrides = collection.ReadSnapshot(&entitlements.mu, func() map[string][]string {
			return collection.CloneSliceValues(entitlements.entitlements)
		})
	}
	return snapshot
}
