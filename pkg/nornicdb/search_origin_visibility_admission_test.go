package nornicdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Author: #1665 implementation lane (Codex). The explicit transaction is
// the deterministic callback gate; DROP runs before the retained action.
func TestSearchOriginDeferredVisibilityDrop(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		t.Run(fmt.Sprint(recreate), func(t *testing.T) {
			db, _ := openConsumerDB(t, false)
			ctx := context.Background()
			const namespace = "visibility_origin"
			store := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), namespace)
			svc, err := db.EnsureSearchIndexes(ctx, namespace, store, SearchIndexBuildWait)
			require.NoError(t, err)
			ddl := cypher.NewStorageExecutor(store)
			ddl.SetSearchService(svc)
			create := "CREATE VECTOR INDEX visibility_vec FOR (n:Doc) ON (n.embedding)"
			_, err = ddl.Execute(ctx, create, nil)
			require.NoError(t, err)
			vector := make([]float64, svc.VectorIndexDimensions())
			query := make([]float32, len(vector))
			vector[0], query[0] = 1, 1
			_, err = store.CreateNode(&storage.Node{ID: "selected", Labels: []string{"Doc"}, Properties: map[string]any{"embedding": vector}})
			require.NoError(t, err)
			_, err = ddl.Execute(ctx, "MATCH (n:Doc) CALL kp.suppress(n)", nil)
			require.NoError(t, err)
			older := cypher.NewStorageExecutor(store)
			older.SetSearchService(svc)
			_, err = older.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			_, err = older.Execute(ctx, "MATCH (n:Doc) WHERE reveal(n) IS NOT NULL CALL kp.unsuppress(n)", nil)
			require.NoError(t, err)
			_, err = ddl.Execute(ctx, "DROP INDEX visibility_vec", nil)
			require.NoError(t, err)
			if recreate {
				_, err = ddl.Execute(ctx, create, nil)
				require.NoError(t, err)
			}
			delivered := make(chan struct{})
			require.NoError(t, storage.SubscribeEvents(db.GetBaseStorageForManager(), storage.EventHandlers{NodeUpdated: func(node *storage.Node) {
				if node.ID == namespace+":selected" {
					close(delivered)
				}
			}}))
			_, err = older.Execute(ctx, "COMMIT", nil)
			require.NoError(t, err)
			select {
			case <-delivered:
			case <-time.After(5 * time.Second):
				t.Fatal("committed visibility action did not publish its notification")
			}
			entry := db.searchServices[namespace]
			require.Eventually(t, func() bool {
				entry.pendingFlushMu.Lock()
				running := entry.pendingFlushRunning
				entry.pendingFlushMu.Unlock()
				entry.pendingMu.Lock()
				idle := len(entry.pendingOps) == 0 && len(entry.refreshRetries) == 0
				entry.pendingMu.Unlock()
				return !running && idle
			}, 5*time.Second, time.Millisecond)
			_, err = store.GetNode("selected")
			require.NoError(t, err, "the committed visibility action must still show the node")
			require.Zero(t, originVectorHits(t, svc, query), "a deferred pre-DROP visibility action rebound its storage notification")
		})
	}
}
