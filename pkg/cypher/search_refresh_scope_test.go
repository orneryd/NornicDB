package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A commit captures one search service for the entire publish. If none was
// available at that point, later resolver changes must not start a partial
// refresh of the committed batch.
func TestCypherRefreshScopeKeepsCapturedNilServiceAfterCommit(t *testing.T) {
	executor := &StorageExecutor{}
	calls := 0
	executor.SetSearchServiceResolver("nornic", func(string) *search.Service {
		calls++
		return &search.Service{}
	})
	scope := cypherRefreshScope{executor: executor}
	svc, deferred, err := scope.PrepareSearchRefresh(context.Background(), search.RefreshRequest{Node: "node", PostCommit: true})
	require.NoError(t, err)
	require.False(t, deferred)
	require.Nil(t, svc)
	require.Zero(t, calls, "a post-commit refresh re-resolved a service after capturing nil")
}

func TestCypherRefreshAfterCommitResolvesServiceOnce(t *testing.T) {
	executor := &StorageExecutor{}
	calls := 0
	executor.SetSearchServiceResolver("nornic", func(string) *search.Service {
		calls++
		return nil
	})
	var tx transactionStorageWrapper
	tx.deferSearchRefresh("node", "edge", storage.CaptureVectorWorkOrigin())
	executor.refreshSearchAfterCommit(&tx)
	require.Equal(t, 1, calls, "the committed batch must use its captured service")
}

func TestCypherRefreshDefersTransactionMutations(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	transaction, err := base.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = transaction.Rollback() })
	tx := transactionStorageWrapper{tx: transaction, underlying: store, namespace: "test", separator: ":"}
	executor := NewStorageExecutor(&tx)
	executor.SetSearchService(&search.Service{})
	var notifications []string
	executor.onNodeMutated = func(id string) { notifications = append(notifications, id) }
	// A value no owner can capture for itself.
	origin := storage.CaptureVectorWorkOrigin() + 1<<40
	ctx := storage.WithVectorWorkOrigin(context.Background(), origin)
	executor.notifyNodeMutated(ctx, "node")
	executor.notifyNodeMutated(ctx, "node")
	executor.notifyEdgeMutated(ctx, "edge")

	// Each deferred refresh keeps the origin admitted for its mutation.
	nodes, edges := tx.takeSearchRefreshes()
	require.Equal(t, map[storage.NodeID]uint64{"node": origin}, nodes)
	require.Equal(t, map[storage.EdgeID]uint64{"edge": origin}, edges)
	require.Equal(t, []string{"node", "node"}, notifications)
}

func TestCypherTransactionRefreshFollowsCommitOrRollback(t *testing.T) {
	for _, commit := range []bool{false, true} {
		name := "rollback"
		if commit {
			name = "commit"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			base := newTestMemoryEngine(t)
			store := storage.NewNamespacedEngine(base, "test")
			_, err := store.CreateNode(&storage.Node{ID: "node", Labels: []string{"Doc"}, Properties: map[string]any{"content": "beforeword"}})
			require.NoError(t, err)
			service := search.NewService(store, 2, nil)
			t.Cleanup(func() { _ = service.Close() })
			require.NoError(t, service.BuildIndexes(ctx))

			transaction, err := base.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = transaction.Rollback() })
			wrapped := &transactionStorageWrapper{tx: transaction, underlying: store, namespace: "test", separator: ":"}
			executor := NewStorageExecutor(wrapped)
			executor.SetSearchService(service)
			_, err = executor.Execute(ctx, "MATCH (n:Doc) SET n.content = 'afterword' RETURN n", nil)
			require.NoError(t, err)
			require.Equal(t, 1, consumerHits(t, service, "beforeword"))
			require.Zero(t, consumerHits(t, service, "afterword"), "uncommitted data reached search")

			if !commit {
				require.NoError(t, transaction.Rollback())
				require.Equal(t, 1, consumerHits(t, service, "beforeword"))
				require.Zero(t, consumerHits(t, service, "afterword"))
				return
			}
			require.NoError(t, transaction.Commit())
			executor.refreshSearchAfterCommit(wrapped)
			require.Zero(t, consumerHits(t, service, "beforeword"))
			require.Equal(t, 1, consumerHits(t, service, "afterword"))
		})
	}
}
