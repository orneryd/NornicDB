package nornicdb

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type dropGenerationRefreshGate struct {
	*refreshReadBarrier
	failOnce atomic.Bool
}

func (g *dropGenerationRefreshGate) GetNode(id storage.NodeID) (*storage.Node, error) {
	node, err := g.refreshReadBarrier.GetNode(id)
	if err == nil && node != nil && g.failOnce.CompareAndSwap(true, false) {
		return nil, errors.New("injected pre-drop refresh read failure")
	}
	return node, err
}

// Pause the real event refresh after its read, before it installs any entries.
// DROP runs while that work is held; releasing it must not restore its vectors.
func TestSearchRefreshBeforeDropCannotRestorePropertyVectors(t *testing.T) {
	for _, test := range []struct {
		name            string
		recreate, retry bool
	}{
		{"dropped", false, false}, {"recreated", true, false}, {"retry_dropped", false, true}, {"retry_recreated", true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, _ := openConsumerDB(t, false)
			ctx := context.Background()
			store := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "dropgeneration")
			barrier := &refreshReadBarrier{Engine: store, observed: storedContent("queuedword"), entered: make(chan struct{}), release: make(chan struct{})}
			t.Cleanup(barrier.open)
			gate := &dropGenerationRefreshGate{refreshReadBarrier: barrier}
			gate.failOnce.Store(test.retry)
			svc, err := db.EnsureSearchIndexes(ctx, "dropgeneration", gate, SearchIndexBuildWait)
			require.NoError(t, err)
			executor := cypher.NewStorageExecutor(store)
			executor.SetSearchService(svc)
			dims := svc.VectorIndexDimensions()
			vector := make([]float64, dims)
			vector[0] = 1
			create := "CREATE VECTOR INDEX causalvec FOR (n:Doc) ON (n.embedding)"
			_, err = executor.Execute(ctx, create, nil)
			require.NoError(t, err)
			barrier.armed.Store(true)
			node := &storage.Node{ID: "late", Labels: []string{"Doc"}, Properties: map[string]any{"content": "queuedword", "embedding": vector, "other": vector}}
			_, err = store.CreateNode(node)
			require.NoError(t, err)
			barrier.wait(t)
			// Seed the index without superseding the live refresh reservation.
			require.NoError(t, svc.IndexNode(node))
			require.Equal(t, 1, svc.CountPropertyVectorEntries("embedding"))
			_, err = executor.Execute(ctx, "DROP INDEX causalvec", nil)
			require.NoError(t, err)
			_, present := store.GetSchema().GetVectorIndex("causalvec")
			require.False(t, present)
			require.Zero(t, svc.CountPropertyVectorEntries("embedding"))
			if test.recreate {
				_, err = executor.Execute(ctx, create, nil)
				require.NoError(t, err)
			}
			barrier.open()
			entry := db.searchServices["dropgeneration"]
			require.Eventually(t, func() bool {
				entry.pendingFlushMu.Lock()
				running := entry.pendingFlushRunning
				entry.pendingFlushMu.Unlock()
				entry.pendingMu.Lock()
				idle := len(entry.pendingOps) == 0 && len(entry.refreshRetries) == 0
				entry.pendingMu.Unlock()
				return !running && idle
			}, 5*time.Second, time.Millisecond)
			require.Zero(t, svc.CountPropertyVectorEntries("embedding"), "a pre-drop refresh restored the retired descriptor's property vectors")
			require.Equal(t, 1, svc.CountPropertyVectorEntries("other"), "unrelated property vectors must survive")
			require.Equal(t, 1, consumerDBHits(t, svc, "queuedword"), "the refresh must retain its text")
			// Work reserved after the drop belongs to the new generation.
			require.NoError(t, svc.RefreshNode("late", storage.CaptureVectorWorkOrigin()))
			require.Equal(t, 1, svc.CountPropertyVectorEntries("embedding"))
		})
	}
}
