package multidb

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNamespaceCreateNativeJournalFailurePublishesNothing(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	base := storage.NewMemoryEngine()
	wal, err := storage.NewWAL(t.TempDir(), storage.DefaultWALConfig())
	require.NoError(t, err)
	manager, err := NewDatabaseManager(storage.NewWALEngine(base, wal), nil)
	require.NoError(t, err)
	defer manager.Close()
	require.NoError(t, wal.Close())
	err = manager.CreateDatabase("unadmitted_create")
	require.ErrorIs(t, err, storage.ErrWALClosed)
	if outcome, ok := err.(interface{ MutationApplied() bool }); ok {
		require.False(t, outcome.MutationApplied())
	}
	require.False(t, manager.Exists("unadmitted_create"), "failed CREATE journal admission changes no catalog state")
	reloaded, err := NewDatabaseManager(base, nil)
	require.NoError(t, err)
	require.False(t, reloaded.Exists("unadmitted_create"))
}
