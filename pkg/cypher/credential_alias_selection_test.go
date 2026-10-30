package cypher

import (
	"context"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCredentialCompositeAliasPublicSelection(t *testing.T) {
	manager, err := multidb.NewDatabaseManager(t.Context(), fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil)
	require.NoError(t, err)
	fixtureowner.Own(t, manager)
	for _, name := range []string{"firstdb", "lastdb", "longsdb", "asciisdb"} {
		require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: name}))
	}
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "probe", []multidb.ConstituentRef{
		{Alias: "Winner", Type: "local", AccessMode: "read_write", DatabaseName: "firstdb"},
		{Alias: "winner", Type: "local", AccessMode: "read_write", DatabaseName: "lastdb"},
		{Alias: "ſ", Type: "local", AccessMode: "read_write", DatabaseName: "longsdb"},
		{Alias: "S", Type: "local", AccessMode: "read_write", DatabaseName: "asciisdb"},
	}))
	adapter := &testDatabaseManagerAdapter{manager: manager}
	engine, err := manager.GetStorage("nornic")
	require.NoError(t, err)
	executor := NewStorageExecutor(engine)
	executor.SetDatabaseManager(adapter)
	for _, selection := range []string{"probe.Winner", "probe.winner", "probe.WINNER"} {
		access, err := AccessDatabases(adapter, selection)
		require.NoError(t, err)
		require.Equal(t, []string{"probe", "lastdb"}, access)
		result, err := executor.Execute(context.Background(), "USE "+selection+" RETURN 1 AS marker", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	}
	for alias, want := range map[string]string{"S": "asciisdb", "s": "asciisdb", "ſ": "longsdb"} {
		access, err := AccessDatabases(adapter, "probe."+alias)
		require.NoError(t, err)
		require.Equal(t, []string{"probe", want}, access)
	}
}
