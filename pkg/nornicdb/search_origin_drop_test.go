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
	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The barrier precedes delivery of the target notification, not its search
// reservation. Physical publication must remain independent of this listener.
func TestSearchOriginQueuedNotificationDrop(t *testing.T) {
	for _, replicated := range []bool{false, true} {
		for _, recreate := range []bool{false, true} {
			for _, retry := range []bool{false, true} {
				for _, newer := range []string{"none", "installed", "pending"} {
					if retry && newer != "none" {
						continue
					} // retry and prior-newer admission are separate causal branches
					t.Run(fmt.Sprintf("replication=%t/recreate=%t/retry=%t/newer=%s", replicated, recreate, retry, newer), func(t *testing.T) {
						db, _ := openConsumerDB(t, false)
						ctx := context.Background()
						ns := "queued_origin"
						base := db.GetBaseStorageForManager()
						store := storage.NewNamespacedEngine(base, ns)
						gate := &originRetryRead{Engine: store}
						gate.fail.Store(retry)
						svc, err := db.EnsureSearchIndexes(ctx, ns, gate, SearchIndexBuildWait)
						require.NoError(t, err)
						exec := cypher.NewStorageExecutor(store)
						exec.SetSearchService(svc)
						create := "CREATE VECTOR INDEX originvec FOR (n:Doc) ON (n.embedding)"
						_, err = exec.Execute(ctx, create, nil)
						require.NoError(t, err)
						vector := make([]float64, svc.VectorIndexDimensions())
						vector[0] = 1
						other := make([]float64, len(vector))
						other[1] = 1
						queryVector := make([]float32, len(vector))
						queryVector[0] = 1
						entered, release := make(chan struct{}), make(chan struct{})
						var once sync.Once
						unblock := func() { once.Do(func() { close(release) }) }
						t.Cleanup(unblock)
						require.NoError(t, storage.SubscribeEvents(base, storage.EventHandlers{NodeCreated: func(n *storage.Node) {
							if string(n.ID) == ns+":blocker" {
								close(entered)
								<-release
							}
						}}))
						blockerDone := make(chan error, 1)
						go func() {
							_, err := store.CreateNode(&storage.Node{ID: "blocker", Labels: []string{"Other"}, Properties: map[string]any{"content": "earlier"}})
							blockerDone <- err
						}()
						select {
						case <-entered:
						case <-time.After(5 * time.Second):
							t.Fatal("earlier listener not entered")
						}
						target := &storage.Node{ID: "target", Labels: []string{"Doc"}, Properties: map[string]any{"content": "originword", "embedding": vector, "other": other}}
						if replicated {
							adapter, err := replication.NewStorageAdapterWithWAL(base, t.TempDir())
							require.NoError(t, err)
							defer adapter.Close()
							copyNode := *target
							copyNode.ID = storage.NodeID(ns + ":target")
							data, err := storage.EncodeEntityTransaction(storage.EntityTransactionRequest{Namespace: ns, LogicalID: "origin-queued", Mode: storage.CommitModeDirect, Operations: []storage.Operation{{Type: storage.OpCreateNode, Node: &copyNode}}})
							require.NoError(t, err)
							require.NoError(t, adapter.ApplyCommand(&replication.Command{Type: replication.CmdCommitTransaction, Data: data, Timestamp: time.Now()}))
						} else {
							_, err = store.CreateNode(target)
							require.NoError(t, err)
						}
						_, err = store.GetNode("target")
						require.NoError(t, err, "target physically visible while its notification is queued")
						require.NoError(t, svc.IndexNode(target))
						_, err = exec.Execute(ctx, "DROP INDEX originvec", nil)
						require.NoError(t, err)
						require.Zero(t, svc.CountPropertyVectorEntries("embedding"))
						if recreate {
							_, err = exec.Execute(ctx, create, nil)
							require.NoError(t, err)
						}
						var pending search.NodeMutation
						if newer == "installed" {
							require.NoError(t, svc.RefreshNode("target", storage.CaptureVectorWorkOrigin()))
						}
						if newer == "pending" {
							pending = svc.ReserveNodeMutation("target", storage.CaptureVectorWorkOrigin())
						}
						unblock()
						require.NoError(t, <-blockerDone)
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
						expected := 0
						if newer == "installed" {
							expected = 1
						}
						require.Equal(t, expected, originVectorHits(t, svc, queryVector), "queued notification changed public vector results after DROP")
						require.Equal(t, expected, svc.CountPropertyVectorEntries("embedding"), "pre-DROP notification restored a retired entry or erased newer work")
						if newer == "pending" {
							require.NoError(t, svc.RefreshNodeWithMutation(pending))
							require.Equal(t, 1, originVectorHits(t, svc, queryVector), "newer admitted work failed to become searchable")
							require.Equal(t, 1, svc.CountPropertyVectorEntries("embedding"), "old event stole the newer pending reservation")
						}
						require.Equal(t, 1, svc.CountPropertyVectorEntries("other"))
						require.Equal(t, 1, consumerDBHits(t, svc, "originword"))
						if retry {
							require.Equal(t, int64(1), gate.failures.Load())
						}
					})
				}
			}
		}
	}
}

func TestSearchOriginDescriptorDomains(t *testing.T) {
	for _, drop := range []string{"rel_vec", "node_vec"} {
		t.Run(drop, func(t *testing.T) {
			db, svc := openConsumerDB(t, true)
			ctx := context.Background()
			store := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), db.defaultDatabaseName())
			exec := cypher.NewStorageExecutor(store)
			exec.SetSearchService(svc)
			vector := make([]float64, svc.VectorIndexDimensions())
			vector[0] = 1
			for _, q := range []string{"CREATE VECTOR INDEX node_vec FOR (n:Doc) ON (n.embedding)", "CREATE VECTOR INDEX rel_vec FOR ()-[r:LINK]-() ON (r.embedding)"} {
				_, err := exec.Execute(ctx, q, nil)
				require.NoError(t, err)
			}
			for _, id := range []storage.NodeID{"a", "b"} {
				_, err := store.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"embedding": vector}})
				require.NoError(t, err)
				require.NoError(t, svc.RefreshNode(id, storage.CaptureVectorWorkOrigin()))
			}
			require.NoError(t, store.CreateEdge(&storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"embedding": vector}}))
			require.NoError(t, svc.RefreshEdge("r", storage.CaptureVectorWorkOrigin()))
			nodeWork := svc.ReserveNodeMutation("a", storage.CaptureVectorWorkOrigin())
			edgeWork := svc.ReserveEdgeMutation("r", storage.CaptureVectorWorkOrigin())
			_, err := exec.Execute(ctx, "DROP INDEX "+drop, nil)
			require.NoError(t, err)
			// Reservations originated before DROP and are applied afterwards.
			require.NoError(t, svc.RefreshNodeWithMutation(nodeWork))
			require.NoError(t, svc.RefreshEdgeWithMutation(edgeWork))
			query := "CALL db.index.vector.queryNodes('node_vec', 10, $v) YIELD node, score RETURN node, score"
			if drop == "node_vec" {
				query = "CALL db.index.vector.queryRelationships('rel_vec', 10, $v) YIELD relationship, score RETURN relationship, score"
			}
			result, err := exec.Execute(ctx, query, map[string]any{"v": vector})
			require.NoError(t, err)
			expected := 2
			if drop == "node_vec" {
				expected = 1
			}
			require.Len(t, result.Rows, expected, "DROP retired the surviving entity domain")
			if drop == "node_vec" {
				require.Zero(t, svc.CountPropertyVectorEntries("embedding"))
			}
		})
	}
}

// A committed Cypher statement can wait in service resolution before its
// post-commit callback reaches search. That callback belongs to the statement,
// not to the later moment at which its search service becomes available.
func TestSearchOriginCypherPostCommitDrop(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		t.Run(fmt.Sprint(recreate), func(t *testing.T) {
			db, svc := openConsumerDB(t, false)
			ctx := context.Background()
			ns := db.defaultDatabaseName()
			store := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), ns)
			ddl := cypher.NewStorageExecutor(store)
			ddl.SetSearchService(svc)
			create := "CREATE VECTOR INDEX postvec FOR (n:Doc) ON (n.embedding)"
			_, err := ddl.Execute(ctx, create, nil)
			require.NoError(t, err)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			mutation := cypher.NewStorageExecutor(store)
			mutation.SetSearchServiceResolver(ns, func(string) *search.Service { close(entered); <-release; return svc })
			vector := make([]float64, svc.VectorIndexDimensions())
			vector[0] = 1
			done := make(chan error, 1)
			go func() {
				_, err := mutation.Execute(ctx, "CREATE (:Doc {content: 'postword', embedding: $v, other: $v})", map[string]any{"v": vector})
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("post-commit service resolution not reached")
			}
			nodes, err := store.GetNodesByLabel("Doc")
			require.NoError(t, err)
			require.Len(t, nodes, 1)
			require.NoError(t, svc.IndexNode(nodes[0]))
			_, err = ddl.Execute(ctx, "DROP INDEX postvec", nil)
			require.NoError(t, err)
			if recreate {
				_, err = ddl.Execute(ctx, create, nil)
				require.NoError(t, err)
			}
			unblock()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("post-commit refresh did not return")
			}
			require.Zero(t, svc.CountPropertyVectorEntries("embedding"), "post-commit callback recaptured the origin")
			require.Equal(t, 1, svc.CountPropertyVectorEntries("other"))
		})
	}
}

func TestSearchOriginLabelSiblingDrop(t *testing.T) {
	db, svc := openConsumerDB(t, false)
	ctx := context.Background()
	store := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), db.defaultDatabaseName())
	exec := cypher.NewStorageExecutor(store)
	exec.SetSearchService(svc)
	for _, q := range []string{"CREATE VECTOR INDEX doc_vec FOR (n:Doc) ON (n.embedding)", "CREATE VECTOR INDEX other_vec FOR (n:Other) ON (n.embedding)"} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}
	vector := make([]float64, svc.VectorIndexDimensions())
	vector[0] = 1
	for id, labels := range map[storage.NodeID][]string{"doc": {"Doc"}, "other": {"Other"}, "both": {"Doc", "Other"}} {
		_, err := store.CreateNode(&storage.Node{ID: id, Labels: labels, Properties: map[string]any{"embedding": vector}})
		require.NoError(t, err)
		require.NoError(t, svc.RefreshNode(id, storage.CaptureVectorWorkOrigin()))
	}
	heldOther, heldBoth := svc.ReserveNodeMutation("other", storage.CaptureVectorWorkOrigin()), svc.ReserveNodeMutation("both", storage.CaptureVectorWorkOrigin())
	_, err := exec.Execute(ctx, "DROP INDEX doc_vec", nil)
	require.NoError(t, err)
	require.Equal(t, 2, svc.CountPropertyVectorEntries("embedding"), "teardown crossed a label or a surviving shared descriptor")
	require.NoError(t, svc.RefreshNodeWithMutation(heldOther))
	require.NoError(t, svc.RefreshNodeWithMutation(heldBoth))
	result, err := exec.Execute(ctx, "CALL db.index.vector.queryNodes('other_vec', 10, $v) YIELD node, score RETURN node, score", map[string]any{"v": vector})
	require.NoError(t, err)
	require.Len(t, result.Rows, 2)
}

type originHeldEmbedder struct {
	unitVectorEmbedder
	entered, release chan struct{}
	once             sync.Once
}

func (p *originHeldEmbedder) wait() { p.once.Do(func() { close(p.entered); <-p.release }) }
func (p *originHeldEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	p.wait()
	return p.unitVectorEmbedder.Embed(ctx, text)
}
func (p *originHeldEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	p.wait()
	return p.unitVectorEmbedder.EmbedBatch(ctx, texts)
}

// Provider selection/claim precedes the provider call, accepted sidecar write,
// notification and search publication. The origin must survive all of them.
func TestSearchOriginEmbeddingOutcomeDrop(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		t.Run(fmt.Sprint(recreate), func(t *testing.T) {
			db := openEmbeddingSearchDB(t, 3)
			ctx := context.Background()
			svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), nil)
			require.NoError(t, err)
			provider := &originHeldEmbedder{unitVectorEmbedder: unitVectorEmbedder{dims: 3}, entered: make(chan struct{}), release: make(chan struct{})}
			var once sync.Once
			unblock := func() { once.Do(func() { close(provider.release) }) }
			t.Cleanup(unblock)
			db.SetEmbedder(provider)
			store := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), db.defaultDatabaseName())
			exec := cypher.NewStorageExecutor(store)
			exec.SetSearchService(svc)
			create := "CREATE VECTOR INDEX provider_vec FOR (n:Doc) ON (n.embedding)"
			_, err = exec.Execute(ctx, create, nil)
			require.NoError(t, err)
			_, err = db.CreateNode(ctx, []string{"Doc"}, map[string]any{"content": "providerword", "embedding": []float64{0, 1, 0}, "other": []float64{0, 0, 1}})
			require.NoError(t, err)
			select {
			case <-provider.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("provider work not admitted")
			}
			_, err = exec.Execute(ctx, "DROP INDEX provider_vec", nil)
			require.NoError(t, err)
			if recreate {
				_, err = exec.Execute(ctx, create, nil)
				require.NoError(t, err)
			}
			unblock()
			require.Eventually(t, func() bool { return db.embedQueue.processed.Load() > 0 }, 5*time.Second, time.Millisecond)
			require.Zero(t, originVectorHits(t, svc, []float32{0, 1, 0}), "held provider outcome made the dropped property searchable again")
			require.Zero(t, svc.CountPropertyVectorEntries("embedding"), "provider continuation lost its selection origin")
			require.Equal(t, 1, svc.CountPropertyVectorEntries("other"))
		})
	}
}

// The one injected storage-read failure sends exactly the same origin through
// the existing delayed retry owner; it never changes the tested ordering.
type originRetryRead struct {
	storage.Engine
	fail     atomic.Bool
	failures atomic.Int64
}

func (g *originRetryRead) GetNode(id storage.NodeID) (*storage.Node, error) {
	if id == "target" && g.fail.CompareAndSwap(true, false) {
		g.failures.Add(1)
		return nil, errors.New("origin regression: injected refresh read failure")
	}
	return g.Engine.GetNode(id)
}

func TestSearchOriginDescriptorAliases(t *testing.T) {
	for _, rebind := range []bool{false, true} {
		t.Run(fmt.Sprintf("rebind=%t", rebind), func(t *testing.T) {
			db, svc := openConsumerDB(t, false)
			ctx := context.Background()
			store := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), db.defaultDatabaseName())
			exec := cypher.NewStorageExecutor(store)
			exec.SetSearchService(svc)
			// Public Cypher refuses duplicate descriptor creation. Native schema APIs
			// and imported definitions can contain aliases, so seed through that public
			// API and exercise DROP and the surviving query through Cypher.
			for _, name := range []string{"alias_a", "alias_b"} {
				require.NoError(t, store.GetSchema().AddVectorIndex(name, "Doc", "embedding", svc.VectorIndexDimensions(), "cosine"))
			}
			v := make([]float64, svc.VectorIndexDimensions())
			v[0] = 1
			_, err := store.CreateNode(&storage.Node{ID: "shared", Labels: []string{"Doc"}, Properties: map[string]any{"embedding": v}})
			require.NoError(t, err)
			require.NoError(t, svc.RefreshNode("shared", storage.CaptureVectorWorkOrigin()))
			held := svc.ReserveNodeMutation("shared", storage.CaptureVectorWorkOrigin())
			if rebind {
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			_, err = exec.Execute(ctx, "DROP INDEX alias_a", nil)
			require.NoError(t, err)
			if rebind {
				_, err = exec.Execute(ctx, "CREATE VECTOR INDEX alias_a FOR (n:Other) ON (n.embedding)", nil)
				require.NoError(t, err)
				_, err = exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
			require.Equal(t, 1, svc.CountPropertyVectorEntries("embedding"), "one name cannot retire a still-owned descriptor")
			require.NoError(t, svc.RefreshNodeWithMutation(held))
			result, err := exec.Execute(ctx, "CALL db.index.vector.queryNodes('alias_b', 1, $v) YIELD node, score RETURN node, score", map[string]any{"v": v})
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			held = svc.ReserveNodeMutation("shared", storage.CaptureVectorWorkOrigin())
			_, err = exec.Execute(ctx, "DROP INDEX alias_b", nil)
			require.NoError(t, err)
			require.NoError(t, svc.RefreshNodeWithMutation(held))
			require.Zero(t, svc.CountPropertyVectorEntries("embedding"))

		})
	}
}

// Empty transaction opening is not mutation admission. Both direct and accepted
// data work admitted after DROP must remain eligible in the new generation.
func TestSearchOriginMutationAdmittedAfterDrop(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		t.Run(fmt.Sprint(recreate), func(t *testing.T) {
			db, svc := openConsumerDB(t, false)
			ctx := context.Background()
			ns := db.defaultDatabaseName()
			base := db.GetBaseStorageForManager()
			store := storage.NewNamespacedEngine(base, ns)
			exec := cypher.NewStorageExecutor(store)
			exec.SetSearchService(svc)
			create := "CREATE VECTOR INDEX admission_vec FOR (n:Doc) ON (n.embedding)"
			_, err := exec.Execute(ctx, create, nil)
			require.NoError(t, err)
			tx, err := storage.BeginEntityTransaction(base, storage.CommitModeExplicit)
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, tx.SetNamespace(ns))
			_, err = exec.Execute(ctx, "DROP INDEX admission_vec", nil)
			require.NoError(t, err)
			if recreate {
				_, err = exec.Execute(ctx, create, nil)
				require.NoError(t, err)
			}
			v := make([]float64, svc.VectorIndexDimensions())
			v[0] = 1
			_, err = tx.CreateNode(&storage.Node{ID: storage.NodeID(ns + ":new_admission"), Labels: []string{"Doc"}, Properties: map[string]any{"embedding": v}})
			require.NoError(t, err)
			require.NoError(t, tx.Commit())
			require.Eventually(t, func() bool { return svc.CountPropertyVectorEntries("embedding") == 1 }, 5*time.Second, time.Millisecond, "post-DROP mutation was incorrectly stamped at empty transaction open")
		})
	}
}

// The wrapper is the public transaction-opening boundary. The command body is
// selected before it waits; DROP executes through the real Cypher executor.
type originTransactionOpenGate struct {
	storage.Engine
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *originTransactionOpenGate) GetInnerEngine() storage.Engine { return g.Engine }
func (g *originTransactionOpenGate) BeginTransaction() (*storage.BadgerTransaction, error) {
	g.once.Do(func() { close(g.entered); <-g.release })
	return storage.BeginEntityTransaction(g.Engine, storage.CommitModeDirect)
}

func TestSearchOriginLeaderPreparationDrop(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		t.Run(fmt.Sprint(recreate), func(t *testing.T) {
			db, _ := openConsumerDB(t, false)
			ctx := context.Background()
			const namespace = "leader_preparation_origin"
			base := db.GetBaseStorageForManager()
			store := storage.NewNamespacedEngine(base, namespace)
			svc, err := db.EnsureSearchIndexes(ctx, namespace, store, SearchIndexBuildWait)
			require.NoError(t, err)
			exec := cypher.NewStorageExecutor(store)
			exec.SetSearchService(svc)
			create := "CREATE VECTOR INDEX leader_origin_vec FOR (n:Doc) ON (n.embedding)"
			_, err = exec.Execute(ctx, create, nil)
			require.NoError(t, err)
			vector := make([]float64, svc.VectorIndexDimensions())
			queryVector := make([]float32, len(vector))
			vector[0], queryVector[0] = 1, 1
			data, err := storage.EncodeNodePayload(&storage.Node{ID: namespace + ":selected", Labels: []string{"Doc"}, Properties: map[string]any{"embedding": vector, "content": "leaderoriginword"}})
			require.NoError(t, err)
			gate := &originTransactionOpenGate{Engine: base, entered: make(chan struct{}), release: make(chan struct{})}
			var once sync.Once
			unblock := func() { once.Do(func() { close(gate.release) }) }
			t.Cleanup(unblock)
			adapter, err := replication.NewStorageAdapterWithWAL(gate, t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { unblock(); _ = adapter.Close() })
			leader := replication.NewStandaloneReplicator(&replication.Config{NodeID: "origin-leader"}, adapter)
			require.NoError(t, leader.Start(ctx))
			t.Cleanup(func() { unblock(); _ = leader.Shutdown() })
			done := make(chan error, 1)
			go func() {
				done <- leader.Apply(&replication.Command{Type: replication.CmdCreateNode, Data: data, Timestamp: time.Now()}, 5*time.Second)
			}()
			select {
			case <-gate.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("leader transaction-open boundary not reached")
			}
			_, err = exec.Execute(ctx, "DROP INDEX leader_origin_vec", nil)
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
				t.Fatal("leader apply did not finish")
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
			require.Zero(t, originVectorHits(t, svc, queryVector), "leader preparation rebound selected command after DROP")
		})
	}
}
