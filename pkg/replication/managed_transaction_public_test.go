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

func TestManagedSchemaTransactionReplicatesAndBackfills(t *testing.T) {
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
	router := &managedRouteReplicator{Replicator: local, peer: remote}
	manager, err := multidb.NewDatabaseManager(NewReplicatedEngine(base, router, time.Second), nil)
	require.NoError(t, err)
	defer manager.Close()
	require.NoError(t, manager.CreateDatabase("schema_command"))
	view, err := manager.GetStorage("schema_command")
	require.NoError(t, err)
	require.NoError(t, view.BulkCreateNodes([]*storage.Node{{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"key": "a"}}, {ID: "b", Labels: []string{"Doc"}, Properties: map[string]any{"key": "b"}}}))
	exec := cypher.NewStorageExecutor(view)
	runSchema := func(statement string) error {
		ctx := context.Background()
		_, err := exec.Execute(ctx, "BEGIN", nil)
		if err != nil {
			return err
		}
		if _, err = exec.Execute(ctx, statement, nil); err != nil {
			_, _ = exec.Execute(ctx, "ROLLBACK", nil)
			return err
		}
		_, err = exec.Execute(ctx, "COMMIT", nil)
		return err
	}
	before := router.calls
	err = runSchema("CREATE INDEX doc_key FOR (n:Doc) ON (n.key)")
	require.NoError(t, err)
	require.Equal(t, before+1, router.calls, "schema-only commit is one replicated atomic command")
	peerView := storage.NewNamespacedEngine(peer, "schema_command")
	check := cypher.NewStorageExecutor(peerView)
	result, err := check.Execute(context.Background(), "MATCH (n:Doc {key:'a'}) RETURN n.key", nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{"a"}}, result.Rows, "replicated index is populated before acknowledgement")
	err = runSchema("DROP INDEX doc_key")
	require.NoError(t, err)
	err = runSchema("CREATE CONSTRAINT doc_unique FOR (n:Doc) REQUIRE n.key IS UNIQUE")
	require.NoError(t, err)
	_, err = peerView.CreateNode(&storage.Node{ID: "duplicate", Labels: []string{"Doc"}, Properties: map[string]any{"key": "a"}})
	require.Error(t, err, "peer constraint includes preexisting nodes")
	router.reject = true
	err = runSchema("CREATE INDEX rejected_other FOR (n:Doc) ON (n.other)")
	require.ErrorIs(t, err, ErrNotLeader)
	require.False(t, view.GetSchema().HasPropertyIndex("Doc", "other"))
	require.False(t, peerView.GetSchema().HasPropertyIndex("Doc", "other"))
}

func TestManagedExplicitTransactionReplicatesMixedWritesAtomically(t *testing.T) {
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
	router := &managedRouteReplicator{Replicator: local, peer: remote}
	manager, err := multidb.NewDatabaseManager(NewReplicatedEngine(base, router, time.Second), nil)
	require.NoError(t, err)
	defer manager.Close()
	require.NoError(t, manager.CreateDatabase("mixed_command"))
	view, err := manager.GetStorage("mixed_command")
	require.NoError(t, err)
	require.NoError(t, view.BulkCreateNodes([]*storage.Node{{ID: "a", Labels: []string{"Identity"}, Properties: map[string]any{"key": "a"}}, {ID: "b", Labels: []string{"Identity"}, Properties: map[string]any{"key": "b"}}}))
	require.NoError(t, view.CreateEdge(&storage.Edge{ID: "old", StartNode: "a", EndNode: "b", Type: "OLD"}))
	require.NoError(t, manager.SetDatabaseLimits("mixed_command", &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 3, MaxEdges: 1}}))
	exec := cypher.NewStorageExecutor(view)
	ctx := context.Background()
	before := router.calls
	for _, statement := range []string{"BEGIN", "CREATE (:Doc {key:'new'})", "MATCH (a:Identity {key:'a'}), (b:Identity {key:'b'}) CREATE (a)-[:NEW]->(b)", "MATCH (a:Identity {key:'a'}) SET a.payload='updated'", "MATCH ()-[r:OLD]->() DELETE r"} {
		_, err = exec.Execute(ctx, statement, nil)
		require.NoError(t, err)
	}
	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
	require.Equal(t, before+1, router.calls, "one atomic command, including update and delete")
	for _, engine := range []storage.Engine{view, storage.NewNamespacedEngine(peer, "mixed_command")} {
		nodes, err := engine.NodeCount()
		require.NoError(t, err)
		require.Equal(t, int64(3), nodes)
		edges, err := engine.EdgeCount()
		require.NoError(t, err)
		require.Equal(t, int64(1), edges)
		node, err := engine.GetNode("a")
		require.NoError(t, err)
		require.Equal(t, "updated", node.Properties["payload"])
		_, err = engine.GetEdge("old")
		require.ErrorIs(t, err, storage.ErrNotFound)
	}
}

func TestManagedNativePendingCommandCanCrossFirstRecount(t *testing.T) {
	base := storage.NewMemoryEngine()
	defer base.Close()
	adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
	require.NoError(t, err)
	defer adapter.Close()
	local := NewStandaloneReplicator(DefaultConfig(), adapter)
	require.NoError(t, local.Start(context.Background()))
	defer local.Shutdown()
	router := &commandMatrixReplicator{Replicator: local, adapter: adapter}
	manager, err := multidb.NewDatabaseManager(NewReplicatedEngine(base, router, time.Second), nil)
	require.NoError(t, err)
	defer manager.Close()
	require.NoError(t, manager.CreateDatabase("native_pending"))
	view, err := manager.GetStorage("native_pending")
	require.NoError(t, err)
	router.outcome = "spare"
	_, err = view.CreateNode(&storage.Node{ID: "first", Labels: []string{"Doc"}})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	manager.GetStorageSize("native_pending")
	_, err = view.CreateNode(&storage.Node{ID: "second", Labels: []string{"Doc"}})
	require.NoError(t, err)
	count, err := view.NodeCount()
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
	tracked, _, _ := manager.GetStorageSize("native_pending")
	recount, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer recount.Close()
	exact, _, _ := recount.GetStorageSize("native_pending")
	require.Equal(t, exact, tracked)
}

func TestManagedTransactionPeerFailureIsAtomicAndNotJournaled(t *testing.T) {
	base := storage.NewMemoryEngine()
	defer base.Close()
	adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
	require.NoError(t, err)
	defer adapter.Close()
	view := storage.NewNamespacedEngine(base, "peer_atomic")
	_, err = view.CreateNode(&storage.Node{ID: "taken", Labels: []string{"Doc"}, Properties: map[string]any{"key": "taken"}})
	require.NoError(t, err)
	exec := cypher.NewStorageExecutor(view)
	err = runSchema("CREATE CONSTRAINT doc_unique FOR (n:Doc) REQUIRE n.key IS UNIQUE")
	require.NoError(t, err)
	require.NoError(t, adapter.FlushWAL())
	before, err := adapter.GetWALPosition()
	require.NoError(t, err)
	replay := storage.ManagedReplay{Namespace: "peer_atomic", Transaction: true, Operations: []storage.Operation{
		{Type: storage.OpCreateNode, Node: &storage.Node{ID: "fresh", Labels: []string{"Doc"}, Properties: map[string]any{"key": "fresh"}}},
		{Type: storage.OpCreateNode, Node: &storage.Node{ID: "duplicate", Labels: []string{"Doc"}, Properties: map[string]any{"key": "taken"}}},
	}}
	data, err := encodeManagedTransaction(replay)
	require.NoError(t, err)
	require.Error(t, adapter.ApplyCommand(&Command{Type: CmdTransaction, Data: data}))
	require.NoError(t, adapter.FlushWAL())
	after, err := adapter.GetWALPosition()
	require.NoError(t, err)
	require.Equal(t, before, after)
	for _, id := range []storage.NodeID{"fresh", "duplicate"} {
		_, err := view.GetNode(id)
		require.ErrorIs(t, err, storage.ErrNotFound)
	}
	count, err := view.NodeCount()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}
