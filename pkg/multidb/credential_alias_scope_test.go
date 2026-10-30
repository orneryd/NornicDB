package multidb

import (
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCredentialAliasMutationsRetireSelectedScope(t *testing.T) {
	tests := []struct {
		name      string
		initial   []ConstituentRef
		remove    bool
		action    string
		reason    string
		wantOwner string
		preValid  bool
	}{
		{
			name: "TestCredentialAliasRemovalRetiresSelectedScope",
			initial: []ConstituentRef{
				{Alias: "Winner", Type: "local", AccessMode: "read_write", DatabaseName: "firstdb"},
				{Alias: "winner", Type: "local", AccessMode: "read_write", DatabaseName: "lastdb"},
			},
			remove:    true,
			action:    "WINNER",
			reason:    "removing a case variant must retire the selected canonical registration",
			wantOwner: "firstdb",
			preValid:  true,
		},
		{
			name: "TestCredentialAliasAdditionRetiresSelectedScope",
			initial: []ConstituentRef{
				{Alias: "Winner", Type: "local", AccessMode: "read_write", DatabaseName: "firstdb"},
			},
			remove:    false,
			action:    "winner",
			reason:    "a new canonical winner must retire the previous registration",
			wantOwner: "lastdb",
			preValid:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager, err := NewDatabaseManager(t.Context(), fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil)
			require.NoError(t, err)
			defer manager.Close()
			for _, name := range []string{"firstdb", "lastdb"} {
				require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: name}))
			}
			require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "probe", tt.initial))
			oldScope, err := manager.StatementCacheScope("probe.WINNER")
			require.NoError(t, err)
			if tt.preValid {
				require.True(t, oldScope.Valid())
			}
			if tt.remove {
				require.NoError(t, manager.RemoveConstituent(t.Context(), "probe", tt.action))
			} else {
				require.NoError(t, manager.AddConstituent(t.Context(), "probe", ConstituentRef{Alias: tt.action, Type: "local", AccessMode: "read_write", DatabaseName: "lastdb"}))
			}
			require.False(t, oldScope.Valid(), tt.reason)
			newScope, err := manager.StatementCacheScope("probe.WINNER")
			require.NoError(t, err)
			require.NotEqual(t, oldScope.Generation(), newScope.Generation())
			owner, err := manager.ResolveConnectionLimitOwner("probe.WINNER")
			require.NoError(t, err)
			require.Equal(t, tt.wantOwner, owner)
		})
	}
}
