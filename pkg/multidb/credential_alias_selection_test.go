package multidb

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCredentialAliasSelectionConsumers(t *testing.T) {
	mgr, err := NewDatabaseManager(storage.NewMemoryEngine(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })
	require.NoError(t, mgr.CreateDatabase("firstdb"))
	require.NoError(t, mgr.CreateDatabase("lastdb"))
	require.NoError(t, mgr.CreateCompositeDatabase("comp", []ConstituentRef{
		{Alias: "Winner", Type: "local", AccessMode: "read_write", DatabaseName: "firstdb"},
		{Alias: "winner", Type: "local", AccessMode: "read_write", DatabaseName: "lastdb"},
	}))
	for _, alias := range []string{"Winner", "winner", "WINNER"} {
		owner, err := mgr.ResolveConnectionLimitOwner("comp." + alias)
		require.NoError(t, err)
		require.Equal(t, "lastdb", owner)
		refs, err := mgr.GetCompositeConstituents("comp")
		require.NoError(t, err)
		require.Len(t, refs, 1)
		require.Equal(t, owner, refs[0].DatabaseName)
	}
	require.NoError(t, mgr.RemoveConstituent("comp", "WINNER"))
	owner, err := mgr.ResolveConnectionLimitOwner("comp.WINNER")
	require.NoError(t, err)
	require.Equal(t, "firstdb", owner)
}
