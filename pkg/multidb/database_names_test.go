package multidb

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestListDatabaseNamesPreservesCatalogPopulation(t *testing.T) {
	inner := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, inner.Close()) })
	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.CreateDatabase("empty_private"))
	require.NoError(t, manager.CreateAlias("selected_alias", "nornic"))
	require.NoError(t, manager.CreateCompositeDatabase("composite", nil))
	snapshot := manager.ListDatabaseNames()
	require.ElementsMatch(t, []string{"nornic", "system", "empty_private", "composite"}, snapshot)
	require.NoError(t, manager.DropDatabase("empty_private"))
	require.ElementsMatch(t, []string{"nornic", "system", "composite"}, manager.ListDatabaseNames())
	// Earlier enumerations are snapshots, not a retained catalog slice.
	require.Contains(t, snapshot, "empty_private")
}
