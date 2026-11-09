package auth

import (
	"github.com/stretchr/testify/require"
	"testing"
)

type catalogMetadataOnlyMode struct{}

func (catalogMetadataOnlyMode) CanSeeDatabase(name string) bool { return name == "visible" }
func (catalogMetadataOnlyMode) CanAccessDatabase(string) bool   { return false }

func TestCatalogVisibilityPreservesMetadataAndSelectionContracts(t *testing.T) {
	mode := catalogMetadataOnlyMode{}
	view := NewCatalogVisibility(mode, nil, func(string, string) bool { return false })
	require.True(t, view.CanSeeDatabase("visible"))
	require.False(t, view.CanSeeDatabase("hidden"))
	accessible, readable := ResolveDatabaseReadAccess(mode, nil, "visible")
	require.False(t, accessible)
	require.False(t, readable, "metadata permission grants no data access")
	scope := NewRequestDatabaseScope("selected", map[string]string{"selected": "selected"})
	require.False(t, NewCatalogVisibility(mode, scope, nil).CanSeeDatabase("visible"), "an explicit selection scope still bounds metadata")
	var trusted CatalogVisibility
	require.True(t, trusted.CanSeeDatabase("embedded"))
}

func TestCatalogVisibilityResolverRemainsLive(t *testing.T) {
	allowed := false
	view := NewCatalogVisibility(nil, nil, func(database, permission string) bool {
		require.Equal(t, "reports", database)
		require.Equal(t, "access", permission)
		return allowed
	})
	require.False(t, view.CanSeeDatabase("reports"))
	allowed = true
	require.True(t, view.CanSeeDatabase("reports"))
}
