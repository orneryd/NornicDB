package replication

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestReplicatedEngine_Standalone_ReplicatesWritesToInnerEngine(t *testing.T) {
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })

	adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = adapter.Close() })

	cfg := DefaultConfig()
	cfg.Mode = ModeStandalone
	cfg.NodeID = "test-node"

	repl, err := NewReplicator(cfg, adapter)
	require.NoError(t, err)
	require.NoError(t, repl.Start(context.Background()))
	t.Cleanup(func() { _ = repl.Shutdown() })

	engine := NewReplicatedEngine(base, repl, 5*time.Second)

	_, err = engine.CreateNode(&storage.Node{ID: "db1:n1", Labels: []string{"Person"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "db2:n2", Labels: []string{"Person"}})
	require.NoError(t, err)

	// Write must be visible on the underlying engine.
	_, err = base.GetNode("db1:n1")
	require.NoError(t, err)

	// Drop prefix must remove only matching IDs.
	_, _, err = engine.DeleteByPrefix("db1:")
	require.NoError(t, err)
	_, err = base.GetNode("db1:n1")
	require.ErrorIs(t, err, storage.ErrNotFound)
	_, err = base.GetNode("db2:n2")
	require.NoError(t, err)
}
