package nornicdb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Query the canonical vector search rather than relying on an entry count.
// Fixtures give the retired property a direction orthogonal to both the
// provider's managed vector and the surviving property.
func originVectorHits(t *testing.T, svc *search.Service, vector []float32) int {
	t.Helper()
	minimum, fallback := 0.99, false
	response, err := svc.Search(context.Background(), "", vector, &search.SearchOptions{Limit: 10, MinSimilarity: &minimum, FallbackEnabled: &fallback})
	require.NoError(t, err)
	return len(response.Results)
}

func openOriginAdmissionDB(t *testing.T) (*DB, *search.Service) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingEnabled, cfg.Memory.DecayEnabled, cfg.Memory.AutoLinksEnabled = true, false, false
	cfg.Memory.EmbeddingDimensions = 3
	cfg.Database.BadgerNodeCacheMaxEntries = 1
	cfg.DeferSearchWarmup = true
	db, err := Open(t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.BuildSearchIndexes(context.Background()))
	svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), nil)
	require.NoError(t, err)
	return db, svc
}

// The public startup gate is an ordering hook: the body mutation and its
// embedding wakeup are admitted before DROP, but no provider selection or
// claim can occur until after it. Waiting for completion is an observation,
// not the ordering mechanism.
func TestSearchOriginEmbeddingQueuedBeforeWorkerDrop(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		t.Run(fmt.Sprint(recreate), func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Memory.EmbeddingEnabled = true
			cfg.Memory.EmbeddingDimensions = 3
			cfg.Memory.DecayEnabled = false
			cfg.Memory.AutoLinksEnabled = false
			cfg.EmbeddingWorker.BatchDelay = 0
			cfg.EmbeddingWorker.TriggerDebounceDelay = 0
			cfg.DeferSearchWarmup = true
			db, err := Open(t.TempDir(), cfg)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			db.SetEmbedder(&unitVectorEmbedder{dims: 3})
			ctx := context.Background()
			require.NoError(t, db.BuildSearchIndexes(ctx))
			svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), nil)
			require.NoError(t, err)
			require.True(t, svc.IsReady())
			store := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), db.defaultDatabaseName())
			exec := cypher.NewStorageExecutor(store)
			exec.SetSearchService(svc)
			create := "CREATE VECTOR INDEX queued_provider_vec FOR (n:Doc) ON (n.embedding)"
			_, err = exec.Execute(ctx, create, nil)
			require.NoError(t, err)
			node, err := db.CreateNode(ctx, []string{"Doc"}, map[string]any{"content": "queuedproviderword", "embedding": []float64{0, 1, 0}, "other": []float64{0, 0, 1}})
			require.NoError(t, err)
			require.NoError(t, svc.RefreshNode(storage.NodeID(node.ID), storage.CaptureVectorWorkOrigin()))
			require.Equal(t, 1, svc.CountPropertyVectorEntries("embedding"))
			require.Zero(t, db.embedQueue.inFlight.Load())
			_, err = exec.Execute(ctx, "DROP INDEX queued_provider_vec", nil)
			require.NoError(t, err)
			require.Zero(t, svc.CountPropertyVectorEntries("embedding"))
			if recreate {
				_, err = exec.Execute(ctx, create, nil)
				require.NoError(t, err)
			}
			db.MarkSearchWarmupReady()
			require.Eventually(t, func() bool { return db.embedQueue.processed.Load() > 0 }, 5*time.Second, time.Millisecond)
			require.Zero(t, originVectorHits(t, svc, []float32{0, 1, 0}), "queued work made the dropped property searchable again")
			require.Zero(t, svc.CountPropertyVectorEntries("embedding"), "queued embedding work acquired its origin at provider selection")
			require.Equal(t, 1, svc.CountPropertyVectorEntries("other"))
		})
	}
}

type originRetryEmbedder struct {
	unitVectorEmbedder
	entered, release chan struct{}
	calls            atomic.Int32
}

func (p *originRetryEmbedder) firstFailure() error {
	if p.calls.Add(1) == 1 {
		close(p.entered)
		<-p.release
		return errors.New("origin regression: first provider result rejected")
	}
	return nil
}

func (p *originRetryEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	if err := p.firstFailure(); err != nil {
		return nil, err
	}
	return p.unitVectorEmbedder.Embed(ctx, text)
}

func (p *originRetryEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	if err := p.firstFailure(); err != nil {
		return nil, err
	}
	return p.unitVectorEmbedder.EmbedBatch(ctx, texts)
}

// DROP precedes the failed first result, so every DLQ claim is necessarily
// post-DROP. The timer's speed cannot change the causal order.
func TestSearchOriginEmbeddingRetryDrop(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		t.Run(fmt.Sprint(recreate), func(t *testing.T) {
			db := openEmbeddingSearchDB(t, 3)
			provider := &originRetryEmbedder{unitVectorEmbedder: unitVectorEmbedder{dims: 3}, entered: make(chan struct{}), release: make(chan struct{})}
			var once sync.Once
			unblock := func() { once.Do(func() { close(provider.release) }) }
			t.Cleanup(unblock)
			db.SetEmbedder(provider)
			ctx := context.Background()
			svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), nil)
			require.NoError(t, err)
			store := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), db.defaultDatabaseName())
			exec := cypher.NewStorageExecutor(store)
			exec.SetSearchService(svc)
			create := "CREATE VECTOR INDEX retry_provider_vec FOR (n:Doc) ON (n.embedding)"
			_, err = exec.Execute(ctx, create, nil)
			require.NoError(t, err)
			_, err = db.CreateNode(ctx, []string{"Doc"}, map[string]any{"content": "retryproviderword", "embedding": []float64{0, 1, 0}, "other": []float64{0, 0, 1}})
			require.NoError(t, err)
			select {
			case <-provider.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("first provider attempt not reached")
			}
			_, err = exec.Execute(ctx, "DROP INDEX retry_provider_vec", nil)
			require.NoError(t, err)
			if recreate {
				_, err = exec.Execute(ctx, create, nil)
				require.NoError(t, err)
			}
			unblock()
			require.Eventually(t, func() bool { return db.embedQueue.processed.Load() > 0 }, 10*time.Second, time.Millisecond)
			require.GreaterOrEqual(t, provider.calls.Load(), int32(2))
			require.Zero(t, originVectorHits(t, svc, []float32{0, 1, 0}), "DLQ continuation made the dropped property searchable again")
			require.Zero(t, svc.CountPropertyVectorEntries("embedding"), "DLQ retry minted a later origin")
			require.Equal(t, 1, svc.CountPropertyVectorEntries("other"))
		})
	}
}

type originParkedReadGate struct {
	*storage.NamespacedEngine
	entered, release chan struct{}
	once             sync.Once
}

func (g *originParkedReadGate) GetNodeWithoutEmbeddings(id storage.NodeID) (*storage.Node, error) {
	g.once.Do(func() { close(g.entered); <-g.release })
	return g.NamespacedEngine.GetNodeWithoutEmbeddings(id)
}

// Administrative retry is a new public admission, independent of when the
// old body was created. Hold its selected-body read across DROP, and also
// prove that a distinct retry admitted after DROP remains usable.
func TestSearchOriginEmbeddingAdministrativeRetryDrop(t *testing.T) {
	for _, beforeDrop := range []bool{true, false} {
		for _, recreate := range []bool{false, true} {
			t.Run(fmt.Sprintf("before=%t/recreate=%t", beforeDrop, recreate), func(t *testing.T) {
				db, svc := openOriginAdmissionDB(t)
				ctx := context.Background()
				store := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), db.defaultDatabaseName())
				exec := cypher.NewStorageExecutor(store)
				exec.SetSearchService(svc)
				vector := make([]float64, svc.VectorIndexDimensions())
				vector[1] = 1
				queryVector := make([]float32, len(vector))
				queryVector[1] = 1
				create := "CREATE VECTOR INDEX admin_retry_vec FOR (n:Doc) ON (n.embedding)"
				_, err := exec.Execute(ctx, create, nil)
				require.NoError(t, err)
				_, err = store.CreateNode(&storage.Node{ID: "parked", Labels: []string{"Doc"}, Properties: map[string]any{"content": "parkedword", "embedding": vector}})
				require.NoError(t, err)
				parked, err := store.GetNode("parked")
				require.NoError(t, err)
				parked.EmbedMeta = map[string]any{"embedding_failed": true}
				require.NoError(t, store.UpdateNodeEmbeddingSidecar(parked))
				gate := &originParkedReadGate{NamespacedEngine: store, entered: make(chan struct{}), release: make(chan struct{})}
				var once sync.Once
				unblock := func() { once.Do(func() { close(gate.release) }) }
				t.Cleanup(unblock)
				config := DefaultEmbedWorkerConfig()
				config.DeferWorkerStart = true
				worker := NewEmbedWorker(&unitVectorEmbedder{dims: len(vector)}, gate, config)
				t.Cleanup(func() { unblock(); worker.Close() })
				failures, err := worker.ParkedEmbeddingFailures(ctx, 0)
				require.NoError(t, err)
				require.Len(t, failures, 1)
				drop := func() {
					_, err := exec.Execute(ctx, "DROP INDEX admin_retry_vec", nil)
					require.NoError(t, err)
					if recreate {
						_, err = exec.Execute(ctx, create, nil)
						require.NoError(t, err)
					}
				}
				if !beforeDrop {
					drop()
				}
				done := make(chan error, 1)
				go func() {
					n, err := worker.RetryParkedEmbeddingFailures(ctx, []storage.NodeID{"parked"})
					if err == nil && n != 1 {
						err = fmt.Errorf("retried %d parked nodes", n)
					}
					done <- err
				}()
				select {
				case <-gate.entered:
				case <-time.After(5 * time.Second):
					t.Fatal("administrative selected-body read not reached")
				}
				if beforeDrop {
					drop()
				}
				unblock()
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(5 * time.Second):
					t.Fatal("administrative retry did not return")
				}
				entry := db.searchServices[db.defaultDatabaseName()]
				require.Eventually(t, func() bool {
					entry.pendingFlushMu.Lock()
					running := entry.pendingFlushRunning
					entry.pendingFlushMu.Unlock()
					entry.pendingMu.Lock()
					idle := len(entry.pendingOps) == 0 && len(entry.refreshRetries) == 0
					entry.pendingMu.Unlock()
					return !running && idle
				}, 5*time.Second, time.Millisecond)
				expected := 1
				if beforeDrop {
					expected = 0
				}
				require.Equal(t, expected, originVectorHits(t, svc, queryVector), "administrative read changed the operation's admission origin")
				worker.StartWorkers()
				require.Eventually(t, func() bool { return worker.Stats().Processed > 0 }, 5*time.Second, time.Millisecond)
				require.Equal(t, expected, originVectorHits(t, svc, queryVector), "administrative MAIN requeue changed the operation's admission origin")
			})
		}
	}
}

// A provider change requeues active/DLQ work without a new body mutation.
// Work admitted after DROP must retain that later identity on this handoff;
// treating all recovered IDs as startup work would suppress valid vectors.
func TestSearchOriginEmbeddingProviderRequeuePreservesNewAdmission(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		for _, fromRetry := range []bool{false, true} {
			t.Run(fmt.Sprintf("DLQ=%t/recovery=%t", fromRetry, recovery), func(t *testing.T) {
				db, svc := openOriginAdmissionDB(t)
				ctx := context.Background()
				store := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), db.defaultDatabaseName())
				exec := cypher.NewStorageExecutor(store)
				exec.SetSearchService(svc)
				for _, query := range []string{"CREATE VECTOR INDEX requeue_vec FOR (n:Doc) ON (n.embedding)", "DROP INDEX requeue_vec", "CREATE VECTOR INDEX requeue_vec FOR (n:Doc) ON (n.embedding)"} {
					_, err := exec.Execute(ctx, query, nil)
					require.NoError(t, err)
				}
				dims := svc.VectorIndexDimensions()
				provider := &originRetryEmbedder{unitVectorEmbedder: unitVectorEmbedder{dims: dims}, entered: make(chan struct{}), release: make(chan struct{})}
				var once sync.Once
				unblock := func() { once.Do(func() { close(provider.release) }) }
				t.Cleanup(unblock)
				config := DefaultEmbedWorkerConfig()
				config.DeferWorkerStart = true
				config.TriggerDebounceDelay = 0
				config.ProviderRetryBackoff, config.ProviderRetryBackoffMax = time.Hour, time.Hour
				worker := NewEmbedWorker(provider, store, config)
				t.Cleanup(func() { unblock(); worker.Close() })
				vector := make([]float64, dims)
				vector[1] = 1
				queryVector := make([]float32, dims)
				queryVector[1] = 1
				_, err := store.CreateNode(&storage.Node{ID: "new_admission", Labels: []string{"Doc"}, Properties: map[string]any{"content": "newadmissionword", "embedding": vector}})
				require.NoError(t, err)
				worker.StartWorkers()
				select {
				case <-provider.entered:
				case <-time.After(5 * time.Second):
					t.Fatal("provider claim not reached")
				}
				require.Eventually(t, func() bool { return originVectorHits(t, svc, queryVector) == 1 }, 5*time.Second, time.Millisecond)
				entry := db.searchServices[db.defaultDatabaseName()]
				require.Eventually(t, func() bool {
					entry.pendingFlushMu.Lock()
					running := entry.pendingFlushRunning
					entry.pendingFlushMu.Unlock()
					entry.pendingMu.Lock()
					idle := len(entry.pendingOps) == 0 && len(entry.refreshRetries) == 0
					entry.pendingMu.Unlock()
					return !running && idle
				}, 5*time.Second, time.Millisecond)
				require.NoError(t, svc.ClearVectorIndex())
				require.Zero(t, originVectorHits(t, svc, queryVector))
				// Evict the selected body's cached copy before requeue, so the
				// next claim must consume the durable marker rather than an old
				// in-memory copy that happened to retain the admission.
				for i := 0; i < 3; i++ {
					_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("cache_control_%d", i)), Labels: []string{"_origin_control"}, Properties: map[string]any{}})
					require.NoError(t, err)
				}
				if recovery {
					// Recovery runs while the original provider still owns its claim.
					// It must not erase that admission during replacement.
					require.Positive(t, store.RefreshPendingEmbeddingsIndex())
				}

				if fromRetry {
					unblock()
					require.Eventually(t, func() bool { stats := worker.Stats(); return stats.Failed > 0 && stats.InFlight == 0 }, 5*time.Second, time.Millisecond)
				}
				worker.SetEmbedder(&unitVectorEmbedder{dims: dims})
				unblock()
				require.Eventually(t, func() bool { return worker.Stats().Processed > 0 }, 5*time.Second, time.Millisecond)
				require.Equal(t, 1, originVectorHits(t, svc, queryVector), "provider replacement lost a valid post-DROP admission")
			})
		}
	}
}

// The administrative operation is admitted before service resolution. Its
// per-node native writes must not acquire a later identity after that wait.
func TestSearchOriginClearEmbeddingsDrop(t *testing.T) {
	for _, captureMode := range []storage.ChangeCaptureMode{storage.ChangeCaptureOff, storage.ChangeCaptureFull} {
		for _, recreate := range []bool{false, true} {
			t.Run(fmt.Sprintf("capture=%s/recreate=%t", captureMode, recreate), func(t *testing.T) {
				db, svc := openConsumerDB(t, false)
				ctx := context.Background()
				base := db.GetBaseStorageForManager()
				if wal, ok := base.(*storage.WALEngine); ok {
					base = wal.GetInnerEngine()
				}
				capture, ok := base.(interface {
					SetChangeCapture(string, storage.ChangeCaptureMode)
				})
				require.True(t, ok)
				capture.SetChangeCapture(db.defaultDatabaseName(), captureMode)
				store := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), db.defaultDatabaseName())
				exec := cypher.NewStorageExecutor(store)
				exec.SetSearchService(svc)
				dims := svc.VectorIndexDimensions()
				vector := make([]float64, dims)
				vector[1] = 1
				queryVector := make([]float32, dims)
				queryVector[1] = 1
				create := "CREATE VECTOR INDEX clear_vec FOR (n:Doc) ON (n.embedding)"
				_, err := exec.Execute(ctx, create, nil)
				require.NoError(t, err)
				_, err = store.CreateNode(&storage.Node{ID: "cleared", Labels: []string{"Doc"}, Properties: map[string]any{"content": "clearword", "embedding": vector}, ChunkEmbeddings: [][]float32{unitVector(dims)}})
				require.NoError(t, err)
				entry := db.searchServices[db.defaultDatabaseName()]
				idle := func() bool {
					entry.pendingFlushMu.Lock()
					running := entry.pendingFlushRunning
					entry.pendingFlushMu.Unlock()
					entry.pendingMu.Lock()
					empty := len(entry.pendingOps) == 0 && len(entry.refreshRetries) == 0
					entry.pendingMu.Unlock()
					return !running && empty
				}
				require.Eventually(t, idle, 5*time.Second, time.Millisecond)
				entered, release := make(chan struct{}), make(chan struct{})
				var once, gate sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				t.Cleanup(unblock)
				// The cached service's resolution consults the reranker resolver;
				// construction settings (SetDbConfigResolver) apply only when a
				// service is created, so they cannot hold an existing one.
				db.SetRerankerResolver(func(string) search.Reranker {
					gate.Do(func() { close(entered); <-release })
					return nil
				})
				done := make(chan error, 1)
				go func() {
					n, err := db.ClearAllEmbeddings()
					if err == nil && n != 1 {
						err = fmt.Errorf("cleared %d nodes", n)
					}
					done <- err
				}()
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("clear operation service resolution not reached")
				}
				_, err = exec.Execute(ctx, "DROP INDEX clear_vec", nil)
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
					t.Fatal("clear operation did not return")
				}
				require.Eventually(t, idle, 5*time.Second, time.Millisecond)
				require.Zero(t, originVectorHits(t, svc, queryVector), "administrative clear rebound each native write after DROP")
			})
		}
	}
}
