package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newPoisonedQueryScanGraph(t *testing.T) *largeEmbeddingGraph {
	t.Helper()
	dir := t.TempDir()
	engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
	fixtureowner.Own(t, engine)
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	manager, err := multidb.NewDatabaseManager(engine, nil)
	require.NoError(t, err)
	require.NoError(t, manager.CreateDatabase("test"))
	require.NoError(t, manager.CreateCompositeDatabase("scans", []multidb.ConstituentRef{
		{Alias: "docs", DatabaseName: "test", Type: "local", AccessMode: "read_write"},
	}))
	g := newLargeEmbeddingGraphOn(t, 4, engine)
	for i := 0; i < 4; i++ {
		g.link(t, storage.NodeID(fmt.Sprintf("doc-%d", i)), "hub", "BACK")
	}
	require.NoError(t, engine.Close())
	// Corrupt offline, then let the engine reopen with its managed commit oracle.
	raw, err := fixtureowner.TrackResult[*badger.DB](t, nil)(badger.Open(badger.DefaultOptions(dir).WithLogger(nil)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	// The fixture's only large values are its four 2048D vectors. Select them
	// by size so the test is independent of the chunk-key layout and metadata.
	require.NoError(t, raw.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		var keys [][]byte
		for it.Rewind(); it.Valid(); it.Next() {
			if it.Item().ValueSize() >= 2048*4 {
				keys = append(keys, it.Item().KeyCopy(nil))
			}
		}
		it.Close()
		require.Len(t, keys, 4)
		for _, key := range keys {
			if err := txn.Set(key, []byte{0xc1}); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, raw.Close())
	engine, err = fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
	fixtureowner.Own(t, engine)
	require.NoError(t, err)
	g.engine = engine
	g.store = storage.NewNamespacedEngine(engine, "test")
	g.exec = NewStorageExecutor(g.store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	manager, err = multidb.NewDatabaseManager(engine, nil)
	require.NoError(t, err)
	g.exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	return g
}

// Property-only scans, bounded traversal seeds and local Fabric fragments
// must not decode stored vectors. Poisoning the chunks exposes hydration.
func TestQueryNodeScansLeaveEmbeddingsInStorage(t *testing.T) {
	g := newPoisonedQueryScanGraph(t)
	docs, hubs := 0, 0
	require.NoError(t, g.engine.StreamNodes(context.Background(), func(node *storage.Node) error {
		for _, label := range node.Labels {
			if label == "Doc" {
				docs++
			}
			if label == "Hub" {
				hubs++
			}
		}
		return nil
	}))
	require.Zero(t, docs, "full hydration drops nodes with unreadable chunks")
	require.Equal(t, 1, hubs)
	for _, explicit := range []bool{false, true} {
		for _, tc := range []struct {
			query string
			rows  int
		}{
			{"MATCH (n) RETURN n.title", 5},
			{"MATCH (n:Doc) RETURN n.title", 4},
			{"MATCH (n:Doc) RETURN n", 4},
			{"MATCH (n) RETURN n", 5},
			{"MATCH (n:Doc) WHERE n.k IN [0,1,2,3] RETURN n.title", 4},
			{"MATCH (n:Doc) RETURN n LIMIT 2", 2},
			{"MATCH (n:Doc)-[:BACK]->(m:Hub) RETURN n.title, m.k LIMIT 2", 2},
			{"MATCH (n)-[:BACK]->(m:Hub) RETURN n.title LIMIT 2", 2},
			{"MATCH (n:Doc)-[:BACK*1..2]->(m) RETURN n.title LIMIT 2", 2},
			{"MATCH (n:Doc)-[:BACK*1..2]->(m) RETURN n.title", 4},
			{"USE scans CALL { USE scans.docs MATCH (n:Doc) RETURN n } RETURN n", 4},
		} {
			t.Run(fmt.Sprintf("tx=%v/%s", explicit, tc.query), func(t *testing.T) {
				if explicit {
					g.run(t, "BEGIN", nil)
					defer g.run(t, "ROLLBACK", nil)
				}
				result, err := g.exec.Execute(context.Background(), tc.query, nil)
				require.NoError(t, err, "a property-only query must not read stored vectors")
				require.Len(t, result.Rows, tc.rows, "property-only reads retain every visible node")
			})
		}
	}
	// Engine wrappers may forward mandatory methods while hiding optional
	// streaming interfaces. Their query-node capability must still be honored.
	g.exec = NewStorageExecutor(&queryNodeReadSpy{Engine: g.store}, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	for _, tc := range []struct {
		query string
		rows  int
	}{
		{"MATCH (n) RETURN n.title", 5},
		{"MATCH (n:Doc) RETURN n", 4},
	} {
		t.Run("wrapper/"+tc.query, func(t *testing.T) {
			result, err := g.exec.Execute(context.Background(), tc.query, nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, tc.rows)
		})
	}
	// A namespaced wrapper that hides optional capabilities still forwards
	// the query scan's metadata-only hydration decision.
	inner := &projectedQueryNodeWrapper{queryNodeReadSpy: &queryNodeReadSpy{Engine: g.engine}}
	wrapped := storage.NewNamespacedEngine(inner, "test")
	exec := NewStorageExecutor(wrapped, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	result, err := exec.Execute(context.Background(), "MATCH (n:Doc) RETURN n LIMIT 2", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 2)
}

type projectedQueryNodeWrapper struct {
	*queryNodeReadSpy
}

func (w *projectedQueryNodeWrapper) StreamNodesByLabelProjected(label string, properties []string, visit func(*storage.Node) error) error {
	return w.Engine.(storage.ProjectedLabelNodeReader).StreamNodesByLabelProjected(label, properties, visit)
}

func TestQueryNodeScanWritesKeepStoredEmbeddings(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("tx=%v", explicit), func(t *testing.T) {
			g := newLargeEmbeddingGraph(t, 2)
			before := g.storedChunks(t, "doc-0")
			if explicit {
				g.run(t, "BEGIN", nil)
			}
			result := g.run(t, "MATCH (n:Doc) SET n.seen = 1 RETURN n.title", nil)
			require.Len(t, result.Rows, 2)
			if explicit {
				g.run(t, "COMMIT", nil)
			}
			require.Equal(t, before, g.storedChunks(t, "doc-0"))
		})
	}
}

func TestQueryNodeLabelScanKeepsTransactionView(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprintf("pending=%v", pending), func(t *testing.T) {
			g := newLargeEmbeddingGraph(t, 2)
			_, err := g.engine.CreateNode(&storage.Node{ID: "other:foreign", Labels: []string{"Doc"}})
			require.NoError(t, err)
			g.run(t, "BEGIN", nil)
			defer g.run(t, "ROLLBACK", nil)
			g.run(t, "MATCH (n:Doc) RETURN n ORDER BY n.title", nil)
			if pending {
				g.run(t, "MATCH (n:Doc {k: 1}) DELETE n", nil)
				g.run(t, "CREATE (:Doc {title: 'pending'})", nil)
			} else {
				require.NoError(t, g.store.DeleteNode("doc-1"))
				_, err := g.store.CreateNode(&storage.Node{ID: "late", Labels: []string{"Doc"}, Properties: map[string]interface{}{"title": "late"}})
				require.NoError(t, err)
			}
			result := g.run(t, "MATCH (n:Doc) RETURN n ORDER BY n.title", nil)
			require.Len(t, result.Rows, 2)
			require.Equal(t, "doc 0", result.Rows[0][0].(*storage.Node).Properties["title"])
			want := "doc 1"
			if pending {
				want = "pending"
			}
			require.Equal(t, want, result.Rows[1][0].(*storage.Node).Properties["title"])
		})
	}
}

func TestFabricFragmentQueryNodeEligibility(t *testing.T) {
	for _, tc := range []struct {
		query    string
		eligible bool
	}{
		{"MATCH (:Hub)-[:HAS]->(n:Doc {k: 0}) RETURN n", true},
		{"MATCH (:Hub)-[:HAS]->(n:Doc {k: 0}) SET n.seen = 1 RETURN n", false},
		{"MATCH (:Hub)-[:HAS]->(n:Doc {k: 0}) CALL db.labels() YIELD label RETURN n LIMIT 1", false},
	} {
		t.Run(tc.query, func(t *testing.T) {
			g := newLargeEmbeddingGraph(t, 2)
			spy := &queryNodeReadSpy{Engine: g.store}
			exec := g.exec.cloneForStorage(spy)
			fabric := &cypherFabricExecutor{base: g.exec}
			_, _, err := fabric.executeFragmentQuery(context.Background(), exec, tc.query, nil)
			require.NoError(t, err)
			require.Equal(t, tc.eligible, exec.readOnlyQueryNodes)
			if tc.eligible {
				require.Positive(t, spy.query.Load())
				require.Zero(t, spy.full.Load())
			} else {
				require.Positive(t, spy.full.Load())
				require.Zero(t, spy.query.Load())
			}
		})
	}
}

func BenchmarkQueryNodeScanReads(b *testing.B) {
	for _, embedded := range []bool{false, true} {
		b.Run(fmt.Sprintf("embedded=%v", embedded), func(b *testing.B) {
			g := newLargeEmbeddingGraph(b, 0)
			for i := 0; i < 100; i++ {
				id := storage.NodeID(fmt.Sprintf("doc-%d", i))
				_, err := g.store.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]interface{}{"title": "title"}})
				require.NoError(b, err)
				if embedded {
					g.embed(b, id, make([]float32, 2048))
				}
			}
			for _, tc := range []struct{ name, query string }{
				{"unlabelled-scalar", "MATCH (n) RETURN n.title"},
				{"labelled-node", "MATCH (n:Doc) RETURN n"},
				{"unlabelled-node", "MATCH (n) RETURN n"},
			} {
				b.Run(tc.name, func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						g.run(b, tc.query, nil)
					}
				})
			}
		})
	}
}
