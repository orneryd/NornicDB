package multidb

import (
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCredentialAliasRemovalRetiresSelectedScope(t *testing.T) {
	manager, err := NewDatabaseManager(t.Context(), fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil)
	require.NoError(t, err)
	defer manager.Close()
	for _, name := range []string{"firstdb", "lastdb"} {
		require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: name}))
	}
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "probe", []ConstituentRef{
		{Alias: "Winner", Type: "local", AccessMode: "read_write", DatabaseName: "firstdb"},
		{Alias: "winner", Type: "local", AccessMode: "read_write", DatabaseName: "lastdb"},
	}))
	oldScope, err := manager.StatementCacheScope("probe.WINNER")
	require.NoError(t, err)
	require.True(t, oldScope.Valid())
	require.NoError(t, manager.RemoveConstituent(t.Context(), "probe", "WINNER"))
	require.False(t, oldScope.Valid(), "removing a case variant must retire the selected canonical registration")
	newScope, err := manager.StatementCacheScope("probe.WINNER")
	require.NoError(t, err)
	require.NotEqual(t, oldScope.Generation(), newScope.Generation())
	owner, err := manager.ResolveConnectionLimitOwner("probe.WINNER")
	require.NoError(t, err)
	require.Equal(t, "firstdb", owner)
}

func TestCredentialAliasAdditionRetiresSelectedScope(t *testing.T) {
	manager, err := NewDatabaseManager(t.Context(), fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil)
	require.NoError(t, err)
	defer manager.Close()
	for _, name := range []string{"firstdb", "lastdb"} {
		require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: name}))
	}
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "probe", []ConstituentRef{
		{Alias: "Winner", Type: "local", AccessMode: "read_write", DatabaseName: "firstdb"},
	}))
	oldScope, err := manager.StatementCacheScope("probe.WINNER")
	require.NoError(t, err)
	require.NoError(t, manager.AddConstituent(t.Context(), "probe", ConstituentRef{Alias: "winner", Type: "local", AccessMode: "read_write", DatabaseName: "lastdb"}))
	require.False(t, oldScope.Valid(), "a new canonical winner must retire the previous registration")
	newScope, err := manager.StatementCacheScope("probe.WINNER")
	require.NoError(t, err)
	require.NotEqual(t, oldScope.Generation(), newScope.Generation())
	owner, err := manager.ResolveConnectionLimitOwner("probe.WINNER")
	require.NoError(t, err)
	require.Equal(t, "lastdb", owner)
}
