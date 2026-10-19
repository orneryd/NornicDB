package replication

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Raft applies a committed entry at every peer, even if its leader-side
// transaction reports a conflict. The wire command must preserve that outcome.
type managedConflictReplicator struct {
	Replicator
	peer      *StorageAdapter
	peerError error
}

func (r *managedConflictReplicator) Apply(cmd *Command, timeout time.Duration) error {
	err := r.Replicator.Apply(cmd, timeout)
	r.peerError = r.peer.ApplyCommand(&Command{Type: cmd.Type, Data: cmd.Data, Timestamp: cmd.Timestamp, RequestID: cmd.RequestID})
	if err != nil {
		return err
	}
	return r.peerError
}

func TestManagedTransactionConflictHasSameOutcomeAtPeer(t *testing.T) {
	base, peer := storage.NewMemoryEngine(), storage.NewMemoryEngine()
	defer base.Close()
	defer peer.Close()
	adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
	require.NoError(t, err)
	defer adapter.Close()
	remote, err := NewStorageAdapterWithWAL(peer, t.TempDir())
	require.NoError(t, err)
	defer remote.Close()
	local := NewStandaloneReplicator(DefaultConfig(), adapter)
	require.NoError(t, local.Start(context.Background()))
	defer local.Shutdown()
	r := &managedConflictReplicator{Replicator: local, peer: remote}
	manager, err := multidb.NewDatabaseManager(NewReplicatedEngine(base, r, time.Second), nil)
	require.NoError(t, err)
	defer manager.Close()
	require.NoError(t, manager.CreateDatabase("replica_conflict"))
	view, err := manager.GetStorage("replica_conflict")
	require.NoError(t, err)
	_, err = view.CreateNode(&storage.Node{ID: "n", Labels: []string{"Doc"}, Properties: map[string]any{"key": "n", "payload": "initial"}})
	require.NoError(t, err)
	exec := cypher.NewStorageExecutor(view)
	ctx := context.Background()
	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (n:Doc {key:'n'}) SET n.payload='stale'", nil)
	require.NoError(t, err)
	node, err := view.GetNode("n")
	require.NoError(t, err)
	node.Properties["payload"] = "winner"
	require.NoError(t, view.UpdateNode(node))
	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.ErrorIs(t, err, storage.ErrConflict)
	require.ErrorIs(t, r.peerError, storage.ErrConflict, "a replicated transaction keeps its original prior-state preconditions")
	for _, engine := range []storage.Engine{view, storage.NewNamespacedEngine(peer, "replica_conflict")} {
		node, err := engine.GetNode("n")
		require.NoError(t, err)
		require.Equal(t, "winner", node.Properties["payload"])
	}
}
