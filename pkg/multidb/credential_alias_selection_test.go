package multidb

import (
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCredentialAliasSelectionConsumers(t *testing.T) {
	mgr, err := NewDatabaseManager(t.Context(), fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil)
	require.NoError(t, err)
	fixtureowner.Own(t, mgr)
	require.NoError(t, mgr.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "firstdb"}))
	require.NoError(t, mgr.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "lastdb"}))
	require.NoError(t, mgr.CreateCompositeDatabase(t.Context(), "comp", []ConstituentRef{
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
	require.NoError(t, mgr.RemoveConstituent(t.Context(), "comp", "WINNER"))
	owner, err := mgr.ResolveConnectionLimitOwner("comp.WINNER")
	require.NoError(t, err)
	require.Equal(t, "firstdb", owner)
}
