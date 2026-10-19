package auth

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestLiveAllowlistModeObservesChanges(t *testing.T) {
	base := storage.NewMemoryEngine()
	engine := storage.NewNamespacedEngine(base, "system")
	defer engine.Close()
	roles := NewRoleStore(engine)
	ctx := context.Background()
	require.NoError(t, roles.CreateRole(ctx, "tenant"))
	store := NewAllowlistStore(engine)
	allows := func(database string) bool { return store.CanAccessDatabaseForRoles([]string{" TENANT "}, database) }
	for _, databases := range [][]string{{"a"}, {"b"}, nil, {"a"}} {
		require.NoError(t, store.SaveRoleDatabases(ctx, "tenant", databases))
		expected := NewAllowlistDatabaseAccessMode(store.Allowlist(), []string{"tenant"})
		for _, database := range []string{"a", "b", "system"} {
			require.Equal(t, expected.CanAccessDatabase(database), allows(database), database)
		}
	}
	require.False(t, store.CanAccessDatabaseForRoles(nil, "a"))
}

func TestRolesAllowMatchesEffectiveEntitlementUnion(t *testing.T) {
	base := storage.NewMemoryEngine()
	engine := storage.NewNamespacedEngine(base, "system")
	defer engine.Close()
	store := NewRoleEntitlementsStore(engine)
	roles := NewRoleStore(engine)
	ctx := context.Background()
	require.NoError(t, roles.CreateRole(ctx, "custom"))
	for _, ids := range [][]string{{"write"}, {"read"}, {"admin"}, nil, {"read", "write"}} {
		require.NoError(t, store.Set(ctx, "custom", ids))
		for _, names := range [][]string{nil, {"viewer"}, {"editor"}, {"admin"}, {"custom"}, {" CUSTOM ", "viewer"}, {"unknown"}} {
			for _, permission := range []string{"read", "write", "admin", "schema", " CREATE ", "unknown"} {
				require.Equal(t, EntitlementsAllow(PermissionsForRoles(names, store), permission), RolesAllow(names, store, permission), "roles=%v permission=%s", names, permission)
			}
		}
	}
}
