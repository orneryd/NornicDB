package bolt

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/catalogadapter"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBoltDatabaseManagerAdapter_ValidAndInvalidConversions(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	mgr, err := multidb.NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgr.Close()
	})

	for _, dbName := range []string{"db1", "db2", "db3", "db4"} {
		require.NoError(t, mgr.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: dbName}))
	}
	require.NoError(t, mgr.CreateCompositeDatabase(t.Context(), "composite1", []multidb.ConstituentRef{
		{DatabaseName: "db1", Alias: "db1", Type: "local", AccessMode: "read_write"},
		{DatabaseName: "db2", Alias: "db2", Type: "local", AccessMode: "read_write"},
	}))

	adapter := &boltDatabaseManagerAdapter{Catalog: catalogadapter.Catalog{Manager: mgr}}

	info := &catalogadapter.InfoView{Info: &multidb.DatabaseInfo{
		Name:      "composite1",
		Type:      "composite",
		Status:    "online",
		IsDefault: false,
		CreatedAt: time.Unix(123, 0).UTC(),
	}}
	assert.Equal(t, "composite1", info.Name())
	assert.Equal(t, "composite", info.Type())
	assert.Equal(t, "online", info.Status())
	assert.False(t, info.IsDefault())
	assert.Equal(t, time.Unix(123, 0).UTC(), info.CreatedAt())

	err = adapter.SetDatabaseLimits(t.Context(), "db1", map[string]interface{}{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid limits type")

	require.NoError(t, adapter.SetDatabaseLimits(t.Context(), "db1", &multidb.Limits{}))
	limits, err := adapter.GetDatabaseLimits("db1")
	require.NoError(t, err)
	assert.IsType(t, &multidb.Limits{}, limits)

	require.NoError(t, adapter.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "db5"}))
	assert.True(t, adapter.Exists("db5"))
	require.NoError(t, adapter.DropDatabase(t.Context(), "db5"))
	assert.False(t, adapter.Exists("db5"))

	require.NoError(t, adapter.CreateAlias(t.Context(), "db1_alias", "db1"))
	aliases := adapter.ListAliases("db1")
	assert.Equal(t, "db1", aliases["db1_alias"])
	require.NoError(t, adapter.DropAlias(t.Context(), "db1_alias"))

	_, err = adapter.GetStorageForUse("db1", "")
	require.NoError(t, err)

	_, err = adapter.ResolveDatabase("db1")
	require.NoError(t, err)

	require.NoError(t, adapter.CreateCompositeDatabase(t.Context(), "composite2", []interface{}{
		multidb.ConstituentRef{DatabaseName: "db1", Alias: "db1a", Type: "local", AccessMode: "read_write"},
		map[string]interface{}{"database_name": "db2", "alias": "db2a", "type": "local", "access_mode": "read_write"},
	}))
	require.True(t, adapter.IsCompositeDatabase("composite2"))

	consecutive, err := adapter.GetCompositeConstituents("composite2")
	require.NoError(t, err)
	require.Len(t, consecutive, 2)

	listed := adapter.ListDatabases()
	require.NotEmpty(t, listed)
	assert.NotEmpty(t, listed[0].Name())
	assert.NotEmpty(t, listed[0].Type())
	assert.NotEmpty(t, listed[0].Status())
	_ = listed[0].CreatedAt()

	composites := adapter.ListCompositeDatabases()
	require.NotEmpty(t, composites)
	assert.Equal(t, "composite", composites[0].Type())

	require.NoError(t, adapter.AddConstituent(t.Context(), "composite2", map[string]interface{}{
		"database_name": "db3",
		"alias":         "db3a",
		"type":          "local",
		"access_mode":   "read_write",
	}))
	require.NoError(t, adapter.AddConstituent(t.Context(), "composite2", multidb.ConstituentRef{
		DatabaseName: "db4",
		Alias:        "db4a",
		Type:         "local",
		AccessMode:   "read_write",
	}))
	consecutive, err = adapter.GetCompositeConstituents("composite2")
	require.NoError(t, err)
	require.Len(t, consecutive, 4)
	require.NoError(t, adapter.RemoveConstituent(t.Context(), "composite2", "db4a"))
	consecutive, err = adapter.GetCompositeConstituents("composite2")
	require.NoError(t, err)
	require.Len(t, consecutive, 3)
	require.NoError(t, adapter.DropCompositeDatabase(t.Context(), "composite2"))
	assert.False(t, adapter.IsCompositeDatabase("composite2"))

	assert.ErrorContains(t, adapter.CreateCompositeDatabase(t.Context(), "broken", []interface{}{123}), "invalid constituent type at index 0")
	assert.ErrorContains(t, adapter.AddConstituent(t.Context(), "composite2", 123), "invalid constituent type")
}
