package storage_test

import (
	"context"
	"errors"
	"fmt"
	fixture "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestPendingDropPublicReadIngress(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, managed := range []bool{false, true} {
			t.Run(fmt.Sprintf("buffered=%v/managed=%v", buffered, managed), func(t *testing.T) {
				b, err := fixture.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: buffered, WriteBehindInterval: time.Hour, LowMemory: true}))
				require.NoError(t, err)
				var tenant, peer storage.Engine = storage.NewNamespacedEngine(b, "tenant"), storage.NewNamespacedEngine(b, "peer")
				var manager *multidb.DatabaseManager
				if managed {
					manager, err = multidb.NewDatabaseManager(t.Context(), b, nil)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, manager.Close()) })
					require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "tenant"}))
					require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "peer"}))
					tenant, err = manager.GetStorage("tenant")
					require.NoError(t, err)
					peer, err = manager.GetStorage("peer")
					require.NoError(t, err)
				}
				for _, engine := range []storage.Engine{tenant, peer} {
					for _, id := range []storage.NodeID{"a", "b"} {
						_, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"ReadFence"}, Properties: map[string]any{"name": string(id)}})
						require.NoError(t, err)
					}
					require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "REL"}))
				}
				require.NoError(t, b.FlushWriteBehind(nil))
				// Work IDs are opaque and deliberately name the opposite
				// namespace. Only their stored graph target controls access.
				require.NoError(t, b.PutDeindexWorkItem(&storage.DeindexWorkItem{WorkItemID: "tenant:opaque-peer", TargetID: "peer:a", Status: "pending"}))
				require.NoError(t, b.PutDeindexWorkItem(&storage.DeindexWorkItem{WorkItemID: "peer:opaque-victim", TargetID: "tenant:a", Status: "pending"}))

				// Warm actual materialized result caches; metrics prove a real public hit.
				makeExecutor := func(engine storage.Engine) (*cypher.StorageExecutor, *observability.CacheMetrics) {
					e := cypher.NewStorageExecutor(engine, cypher.QueryCachePolicy{MaxEntries: 16, TTL: time.Minute})
					metrics := observability.NewCacheMetrics(prometheus.NewRegistry())
					e.SetCacheMetrics(metrics)
					return e, metrics
				}
				victimExec, victimCache := makeExecutor(tenant)
				peerExec, peerCache := makeExecutor(peer)
				const query = "MATCH (n:ReadFence) RETURN n.name"
				for _, e := range []*cypher.StorageExecutor{victimExec, peerExec} {
					for i := 0; i < 2; i++ {
						result, err := e.Execute(context.Background(), query, nil)
						require.NoError(t, err)
						require.Len(t, result.Rows, 2)
					}
				}
				require.Equal(t, float64(1), testutil.ToFloat64(victimCache.Hits.WithLabelValues("query_result")))
				require.Equal(t, float64(1), testutil.ToFloat64(peerCache.Hits.WithLabelValues("query_result")))
				victimTx, err := b.BeginTransaction()
				require.NoError(t, err)
				defer victimTx.Rollback()
				require.NoError(t, victimTx.SetNamespace("tenant"))
				_, err = victimTx.GetNode("tenant:a")
				require.NoError(t, err)
				peerTx, err := b.BeginTransaction()
				require.NoError(t, err)
				defer peerTx.Rollback()
				require.NoError(t, peerTx.SetNamespace("peer"))
				_, err = peerTx.GetNode("peer:a")
				require.NoError(t, err)
				oldSnapshot, err := storage.OpenReadSnapshot(context.Background(), tenant)
				require.NoError(t, err)
				defer oldSnapshot.Close()
				_, err = tenant.GetNode("a")
				require.NoError(t, err)
				_, err = tenant.NodeCount()
				require.NoError(t, err)
				failure := errors.New("read ingress intrinsic cleanup unavailable")
				storage.ReviewSetDropTailFailure(b, func(string) error { return failure })
				t.Cleanup(func() { storage.ReviewSetDropTailFailure(b, nil) })
				if managed {
					// An applied DROP is accepted work: its failed cleanup is
					// reported as an accepted outcome whose cause is the failure.
					err = manager.DropDatabase(t.Context(), "tenant")
					var accepted *storage.AcceptedCommitError
					require.ErrorAs(t, err, &accepted)
					require.ErrorIs(t, accepted.Cause(), failure)
				} else {
					_, _, err = b.DeleteByPrefix("tenant:")
					require.ErrorIs(t, err, failure)
				}
				peerWork, workErr := b.GetDeindexWorkItem("tenant:opaque-peer")
				require.NoError(t, workErr)
				require.NotNil(t, peerWork)
				require.Equal(t, "peer:a", peerWork.TargetID)
				_, workErr = b.GetDeindexWorkItem("peer:opaque-victim")
				require.ErrorIs(t, workErr, storage.ErrConflict)

				for _, read := range []struct {
					name string
					run  func() error
				}{
					{"warm node", func() error { _, e := tenant.GetNode("a"); return e }},
					{"warm graph count", func() error { _, e := tenant.NodeCount(); return e }},
					{"global count", func() error { _, e := b.NodeCount(); return e }},
					{"latest fallback", func() error { _, e := b.GetNodeLatestVisible("tenant:a"); return e }},
					{"bounded latest", func() error { _, e := b.BatchGetNodesLatestVisible([]storage.NodeID{"tenant:a"}); return e }},
					{"raw partial prefix", func() error {
						return b.StreamNodesByPrefix(context.Background(), "ten", func(*storage.Node) error { return errors.New("pending prefix emitted a body") })
					}},
					{"global stream", func() error {
						return b.StreamNodes(context.Background(), func(*storage.Node) error { return errors.New("global pending view emitted a body") })
					}},
					{"old snapshot", func() error {
						emitted := false
						err := oldSnapshot.IterateNodes(func(*storage.Node) bool { emitted = true; return false })
						if emitted {
							return errors.New("pending old snapshot emitted a body")
						}
						return err
					}},
					{"new snapshot", func() error {
						s, e := storage.OpenReadSnapshot(context.Background(), tenant)
						if s != nil {
							s.Close()
						}
						return e
					}},
					{"warm transaction point", func() error { _, e := victimTx.GetNode("tenant:a"); return e }},
					{"transaction label cache", func() error { _, e := victimTx.GetNodesByLabel("ReadFence"); return e }},
				} {
					t.Run(read.name, func(t *testing.T) { require.ErrorIs(t, read.run(), storage.ErrConflict) })
				}
				result, err := victimExec.Execute(context.Background(), query, nil)
				require.ErrorIs(t, err, storage.ErrConflict)
				require.Nil(t, result)
				require.Equal(t, float64(1), testutil.ToFloat64(victimCache.Hits.WithLabelValues("query_result")), "pending graph must not return old cached data")
				result, err = peerExec.Execute(context.Background(), query, nil)
				require.NoError(t, err)
				require.Len(t, result.Rows, 2)
				require.Equal(t, float64(2), testutil.ToFloat64(peerCache.Hits.WithLabelValues("query_result")), "peer cached query remains available")
				_, err = peer.GetNode("a")
				require.NoError(t, err)
				count, err := peer.NodeCount()
				require.NoError(t, err)
				require.EqualValues(t, 2, count)
				batch, err := b.BatchGetNodes([]storage.NodeID{"peer:a", "peer:b"})
				require.NoError(t, err)
				require.Len(t, batch, 2)
				batch, err = b.BatchGetNodesLatestVisible([]storage.NodeID{"peer:a"})
				require.NoError(t, err)
				require.Len(t, batch, 1)
				count, err = b.NodeCountByLabelInNamespace("ten", "ReadFence")
				require.NoError(t, err)
				require.Zero(t, count, "logical namespace ten does not mean raw prefix ten")
				var ids []storage.NodeID
				require.NoError(t, b.StreamNodesByPrefix(context.Background(), "peer:", func(n *storage.Node) error { ids = append(ids, n.ID); return nil }))
				require.Len(t, ids, 2)
				// These low-level transaction collection methods remain global,
				// even after a write or SetNamespace pins the transaction.
				_, err = peerTx.AllNodes()
				require.ErrorIs(t, err, storage.ErrConflict)
				_, err = peerTx.AllEdges()
				require.ErrorIs(t, err, storage.ErrConflict)
				_, err = peerTx.GetEdgesByType("REL")
				require.ErrorIs(t, err, storage.ErrConflict)
				nodes, err := peerTx.GetNodesByLabel("ReadFence")
				require.NoError(t, err)
				require.Len(t, nodes, 2)
				peerSnapshot, err := storage.OpenReadSnapshot(context.Background(), peer)
				require.NoError(t, err)
				defer peerSnapshot.Close()
				nodes = nil
				require.NoError(t, peerSnapshot.IterateNodes(func(n *storage.Node) bool { nodes = append(nodes, n); return true }))
				require.Len(t, nodes, 2)
			})
		}
	}
}

type dropSnapshotDecorator struct {
	storage.Engine
	calls int
}

func (d *dropSnapshotDecorator) OpenReadSnapshot(ctx context.Context) (storage.ReadSnapshotView, error) {
	d.calls++
	return storage.OpenReadSnapshot(ctx, d.Engine)
}
func (d *dropSnapshotDecorator) GetInnerEngine() storage.Engine { return nil }
func TestPendingDropSnapshotUsesActualDecoratorAndNestedPrefix(t *testing.T) {
	b, err := fixture.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	for _, id := range []storage.NodeID{"tenant:a", "peer:inner:child", "peer:outside", "foreign:other"} {
		_, err = b.CreateNode(&storage.Node{ID: id})
		require.NoError(t, err)
	}
	storage.ReviewSetDropTailFailure(b, func(string) error { return errors.New("snapshot fixture tail failure") })
	t.Cleanup(func() { storage.ReviewSetDropTailFailure(b, nil) })
	_, _, err = b.DeleteByPrefix("tenant:")
	require.Error(t, err)
	decorator := &dropSnapshotDecorator{Engine: b}
	nested := storage.NewNamespacedEngine(storage.NewNamespacedEngine(decorator, "peer"), "inner")
	snapshot, err := storage.OpenReadSnapshot(context.Background(), nested)
	require.NoError(t, err)
	defer snapshot.Close()
	require.Equal(t, 1, decorator.calls)
	var ids []storage.NodeID
	require.NoError(t, snapshot.IterateNodes(func(n *storage.Node) bool { ids = append(ids, n.ID); return true }))
	require.Equal(t, []storage.NodeID{"child"}, ids)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = storage.OpenReadSnapshot(ctx, nested)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 2, decorator.calls)
}

func TestPendingDropClosedReadKeepsLifecycleError(t *testing.T) {
	b, err := fixture.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	_, err = b.CreateNode(&storage.Node{ID: "tenant:a"})
	require.NoError(t, err)
	storage.ReviewSetDropTailFailure(b, func(string) error { return errors.New("closed reader fixture tail failure") })
	_, _, err = b.DeleteByPrefix("tenant:")
	require.Error(t, err)
	storage.ReviewSetDropTailFailure(b, nil)
	require.NoError(t, b.Close())
	_, err = b.NodeCount()
	require.ErrorIs(t, err, storage.ErrStorageClosed)
	_, err = b.NodeCountByPrefix("tenant:")
	require.ErrorIs(t, err, storage.ErrStorageClosed)
	_, err = b.GetNode("tenant:a")
	require.ErrorIs(t, err, storage.ErrStorageClosed)
	_, err = b.GetNodeLatestVisible("tenant:a")
	require.ErrorIs(t, err, storage.ErrStorageClosed)
}

func TestPendingDropUnboundTransactionPrefixReadsRemainPeerScoped(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, query := range []bool{false, true} {
			t.Run(fmt.Sprintf("buffered=%v/query=%v", buffered, query), func(t *testing.T) {
				b, err := fixture.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: buffered, WriteBehindInterval: time.Hour, LowMemory: true}))
				require.NoError(t, err)
				for _, id := range []storage.NodeID{"tenant:a", "peer:keep", "foreign:other"} {
					_, err = b.CreateNode(&storage.Node{ID: id, Labels: []string{"ReadFence"}})
					require.NoError(t, err)
				}
				require.NoError(t, b.FlushWriteBehind(nil))
				storage.ReviewSetDropTailFailure(b, func(string) error { return errors.New("unbound prefix fixture tail failure") })
				t.Cleanup(func() { storage.ReviewSetDropTailFailure(b, nil) })
				_, _, err = b.DeleteByPrefix("tenant:")
				require.Error(t, err)
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				opts := storage.StreamNodesOptions{Prefix: "peer:", WithEmbeddings: !query, QueryNodes: query}
				if query {
					opts.Label = "ReadFence"
				}
				var ids []storage.NodeID
				err = tx.StreamNodesWithOptions(context.Background(), opts, func(n *storage.Node) error { ids = append(ids, n.ID); return nil })
				require.NoError(t, err)
				require.Equal(t, []storage.NodeID{"peer:keep"}, ids)
				opts.Prefix = "foreign:"
				ids = nil
				require.NoError(t, tx.StreamNodesWithOptions(context.Background(), opts, func(n *storage.Node) error { ids = append(ids, n.ID); return nil }))
				require.Equal(t, []storage.NodeID{"foreign:other"}, ids, "one narrowed label cache must not masquerade as another complete scope")
				opts.Prefix = "peer:"
				ids = nil
				require.NoError(t, tx.StreamNodesWithOptions(context.Background(), opts, func(n *storage.Node) error { ids = append(ids, n.ID); return nil }))
				require.Equal(t, []storage.NodeID{"peer:keep"}, ids)

				_, err = tx.AllNodes()
				require.ErrorIs(t, err, storage.ErrConflict, "an explicit peer prefix must not turn the unbound frame into a global-read exemption")
				_, err = tx.AllEdges()
				require.ErrorIs(t, err, storage.ErrConflict)
			})
		}
	}
}
