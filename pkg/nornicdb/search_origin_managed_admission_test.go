package nornicdb

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Author: #1665 implementation lane (Codex). Hold the retained managed write
// policy after public admission/qualification, before the native transaction
// stages its body. There is no sleep or extra product hook.
type originManagedWriteGate struct {
	storage.Engine
	entered, release chan struct{}
	once             sync.Once
}

func (g *originManagedWriteGate) GetInnerEngine() storage.Engine { return g.Engine }
func (g *originManagedWriteGate) WithEntityChange(op storage.Operation, apply func() error) error {
	if op.Node != nil && op.Node.ID == "managed_origin:selected" {
		g.once.Do(func() { close(g.entered); <-g.release })
	}
	return apply()
}

func TestSearchOriginManagedPreparationDrop(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		t.Run(map[bool]string{false: "drop", true: "recreate"}[recreate], func(t *testing.T) {
			db, _ := openConsumerDB(t, false)
			ctx := context.Background()
			gate := &originManagedWriteGate{Engine: db.GetBaseStorageForManager(), entered: make(chan struct{}), release: make(chan struct{})}
			var once sync.Once
			unblock := func() { once.Do(func() { close(gate.release) }) }
			t.Cleanup(unblock)
			manager, err := multidb.NewDatabaseManager(ctx, gate, nil)
			require.NoError(t, err)
			t.Cleanup(func() { unblock(); _ = manager.Close() })
			const ns = "managed_origin"
			require.NoError(t, manager.CreateDatabase(ctx, multidb.DatabaseCreateRequest{Name: ns}))
			require.NoError(t, manager.SetDatabaseLimits(ctx, ns, &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 100}}))
			store, err := manager.GetStorage(ns)
			require.NoError(t, err)
			svc, err := db.EnsureSearchIndexes(ctx, ns, store, SearchIndexBuildWait)
			require.NoError(t, err)
			exec := cypher.NewStorageExecutor(store)
			exec.SetSearchService(svc)
			create := "CREATE VECTOR INDEX managed_origin_vec FOR (n:Doc) ON (n.embedding)"
			_, err = exec.Execute(ctx, create, nil)
			require.NoError(t, err)
			vector := make([]float64, svc.VectorIndexDimensions())
			query := make([]float32, len(vector))
			vector[0], query[0] = 1, 1
			delivered := make(chan struct{})
			require.NoError(t, storage.SubscribeEvents(db.GetBaseStorageForManager(), storage.EventHandlers{NodeCreated: func(node *storage.Node) {
				if node.ID == ns+":selected" {
					close(delivered)
				}
			}}))
			done := make(chan error, 1)
			go func() {
				_, err := store.CreateNode(&storage.Node{ID: "selected", Labels: []string{"Doc"}, Properties: map[string]any{"embedding": vector, "content": "managedoriginword"}})
				done <- err
			}()
			select {
			case <-gate.entered:
			case err := <-done:
				require.NoError(t, err)
				t.Fatal("managed admission skipped its write-policy boundary")
			case <-time.After(5 * time.Second):
				t.Fatal("managed write-policy boundary not reached")
			}
			_, err = exec.Execute(ctx, "DROP INDEX managed_origin_vec", nil)
			require.NoError(t, err)
			if recreate {
				_, err = exec.Execute(ctx, create, nil)
				require.NoError(t, err)
			}
			unblock()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("managed write did not finish")
			}
			select {
			case <-delivered:
			case <-time.After(5 * time.Second):
				t.Fatal("managed publication listener did not finish")
			}
			entry := db.searchServices[ns]
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
			require.NoError(t, err, "the managed mutation must still commit")
			require.Zero(t, originVectorHits(t, svc, query), "managed preparation rebound the pre-DROP admission at native staging")
		})
	}
}
