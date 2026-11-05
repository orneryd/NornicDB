package catalogadapter

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCatalogViewsRetainManagerSnapshotAndErrors(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	manager, err := multidb.NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	fixtureowner.OwnChecked(t, manager)
	catalog := Catalog{Manager: manager}
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "view_tenant"}))
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "view_composite", []multidb.ConstituentRef{{Alias: "local", DatabaseName: "view_tenant", Type: "local", AccessMode: "read"}}))
	for _, selection := range []struct {
		records []*multidb.DatabaseInfo
		views   []DatabaseInfoInterface
	}{
		{manager.ListDatabases(), catalog.ListDatabases()},
		{manager.ListCompositeDatabases(), catalog.ListCompositeDatabases()},
	} {
		require.Len(t, selection.views, len(selection.records))
		byName := make(map[string]DatabaseInfoInterface, len(selection.views))
		for _, view := range selection.views {
			byName[view.Name()] = view
		}
		require.Len(t, byName, len(selection.records))
		for _, record := range selection.records {
			view := byName[record.Name]
			require.NotNil(t, view)
			require.Equal(t, record.Name, view.Name())
			require.Equal(t, record.Type, view.Type())
			require.Equal(t, record.Status, view.Status())
			require.Equal(t, record.IsDefault, view.IsDefault())
			require.Equal(t, record.CreatedAt, view.CreatedAt())
		}
	}
	stored, err := manager.GetCompositeConstituents("view_composite")
	require.NoError(t, err)
	projected, err := catalog.GetCompositeConstituents("view_composite")
	require.NoError(t, err)
	require.Len(t, projected, len(stored))
	for i, ref := range stored {
		require.Equal(t, ref, projected[i])
	}
	_, expected := manager.GetCompositeConstituents("absent")
	views, actual := catalog.GetCompositeConstituents("absent")
	require.Error(t, expected)
	require.EqualError(t, actual, expected.Error())
	require.Nil(t, views)
}
