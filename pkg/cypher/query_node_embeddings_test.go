package cypher

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// largeEmbeddingGraph is a hub linked to docs that carry a 2048-dimension
// managed embedding, written as the embed worker writes it: in the embedding
// sidecar, outside the node body.
type largeEmbeddingGraph struct {
	engine *storage.BadgerEngine
	store  storage.Engine
	exec   *StorageExecutor
}

func newLargeEmbeddingGraph(tb testing.TB, docs int) *largeEmbeddingGraph {
	tb.Helper()
	return newLargeEmbeddingGraphOn(tb, docs, newTestMemoryEngine(tb).BadgerEngine)
}

// newColdLargeEmbeddingGraph is newLargeEmbeddingGraph on an engine whose node
// cache holds almost nothing, as for a graph larger than the cache: reads go
// to the stored body and chunks.
func newColdLargeEmbeddingGraph(tb testing.TB, docs int) *largeEmbeddingGraph {
	tb.Helper()
	engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{InMemory: true, NodeCacheMaxEntries: 1})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = engine.Close() })
	return newLargeEmbeddingGraphOn(tb, docs, engine)
}

func newLargeEmbeddingGraphOn(tb testing.TB, docs int, engine *storage.BadgerEngine) *largeEmbeddingGraph {
	tb.Helper()
	// "seen" doesn't feed the embedding text, so setting it keeps the
	// embedding (#963).
	engine.SetEmbeddingTextPolicy(storage.EmbeddingTextPolicy{Exclude: []string{"seen"}, IncludeLabels: true})
	g := &largeEmbeddingGraph{engine: engine, store: storage.NewNamespacedEngine(engine, "test")}
	g.exec = NewStorageExecutorWithQueryCachePolicy(g.store, 0, 0)
	g.run(tb, "CREATE INDEX doc_k FOR (n:Doc) ON (n.k)", nil)
	if _, err := g.store.CreateNode(&storage.Node{ID: "hub", Labels: []string{"Hub"}, Properties: map[string]interface{}{"k": int64(0)}}); err != nil {
		tb.Fatal(err)
	}
	for i := 0; i < docs; i++ {
		id := storage.NodeID(fmt.Sprintf("doc-%d", i))
		if _, err := g.store.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]interface{}{"k": int64(i), "title": fmt.Sprintf("doc %d", i)}}); err != nil {
			tb.Fatal(err)
		}
		vector := make([]float32, 2048)
		vector[i%len(vector)] = 1
		g.embed(tb, id, vector)
		g.link(tb, "hub", id, "HAS")
	}
	return g
}

func (g *largeEmbeddingGraph) run(tb testing.TB, query string, params map[string]interface{}) *ExecuteResult {
	tb.Helper()
	result, err := g.exec.Execute(context.Background(), query, params)
	if err != nil {
		tb.Fatal(err)
	}
	return result
}

func (g *largeEmbeddingGraph) link(tb testing.TB, from, to storage.NodeID, relType string) {
	tb.Helper()
	if err := g.store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("%s-%s-%s", from, relType, to)), StartNode: from, EndNode: to, Type: relType}); err != nil {
		tb.Fatal(err)
	}
}

// embed writes vector as id's managed embedding through the worker sidecar.
func (g *largeEmbeddingGraph) embed(tb testing.TB, id storage.NodeID, vector []float32) {
	tb.Helper()
	stored, err := g.engine.GetNode("test:" + id)
	if err != nil {
		tb.Fatal(err)
	}
	payload := storage.CopyNode(stored)
	payload.ChunkEmbeddings = [][]float32{vector}
	payload.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": 1}
	if err := g.engine.UpdateNodeEmbeddingSidecar(payload); err != nil {
		tb.Fatal(err)
	}
}

func (g *largeEmbeddingGraph) storedChunks(t *testing.T, id storage.NodeID) [][]float32 {
	t.Helper()
	node, err := g.store.GetNode(id)
	require.NoError(t, err)
	return node.ChunkEmbeddings
}

// TestQueryNodeReadsLeaveEmbeddingsInStorage: in a transaction, the nodes a
// statement binds, by index seek, traversal or relationship endpoint, carry
// their properties and embedding metadata but not their stored vectors, which
// the rows never read; n.has_embedding still answers from the metadata.
func TestQueryNodeReadsLeaveEmbeddingsInStorage(t *testing.T) {
	g := newLargeEmbeddingGraph(t, 4)
	g.run(t, "BEGIN", nil)
	defer g.run(t, "ROLLBACK", nil)
	for _, query := range []string{
		"MATCH (n:Doc {k: 3}) RETURN n",
		"MATCH (n:Doc) WHERE n.k IN [3] RETURN n",
		"MATCH (:Hub {k: 0})-[:HAS]->(n:Doc {k: 3}) RETURN n",
		"MATCH (:Hub)-[r:HAS]->(:Doc {k: 3}) RETURN endNode(r) AS n",
	} {
		t.Run(query, func(t *testing.T) {
			result := g.run(t, query, nil)
			require.Len(t, result.Rows, 1)
			node, ok := result.Rows[0][0].(*storage.Node)
			require.True(t, ok, "row value %T", result.Rows[0][0])
			require.Equal(t, "doc 3", node.Properties["title"])
			require.Zero(t, len(node.ChunkEmbeddings))
			require.Equal(t, true, node.EmbedMeta["has_embedding"])
		})
	}
	require.Equal(t, [][]interface{}{{true}}, g.run(t, "MATCH (n:Doc {k: 3}) RETURN n.has_embedding", nil).Rows)
}

// TestQueryNodeWritesKeepStoredEmbeddings: a SET written from a node a
// statement read without its vectors keeps the stored embedding, in an
// auto-commit statement and in an explicit transaction, for a worker sidecar
// embedding and for chunks a large node body stored apart.
func TestQueryNodeWritesKeepStoredEmbeddings(t *testing.T) {
	g := newLargeEmbeddingGraph(t, 4)
	large := make([][]float32, 4)
	for c := range large {
		large[c] = make([]float32, 4000) // ~64KB in all: stored apart from the body
		large[c][c] = 1
	}
	_, err := g.store.CreateNode(&storage.Node{ID: "big", Labels: []string{"Doc"}, Properties: map[string]interface{}{"k": int64(100), "title": "big"}, ChunkEmbeddings: large})
	require.NoError(t, err)
	g.link(t, "hub", "big", "HAS")

	for _, tc := range []struct {
		id storage.NodeID
		k  int64
	}{{"doc-3", 3}, {"big", 100}} {
		want := g.storedChunks(t, tc.id)
		require.NotEmpty(t, want)
		params := map[string]interface{}{"k": tc.k}
		g.run(t, "MATCH (n:Doc {k: $k}) SET n.seen = 1", params)
		require.Equal(t, want, g.storedChunks(t, tc.id))
		g.run(t, "MATCH (:Hub)-[:HAS]->(n:Doc {k: $k}) SET n.seen = 2", params)
		require.Equal(t, want, g.storedChunks(t, tc.id))
		g.run(t, "BEGIN", nil)
		g.run(t, "MATCH (n:Doc {k: $k}) SET n.seen = 3", params)
		g.run(t, "MATCH (:Hub)-[r:HAS]->(:Doc {k: $k}) WITH endNode(r) AS n SET n.seen = 4", params)
		g.run(t, "COMMIT", nil)
		require.Equal(t, want, g.storedChunks(t, tc.id))
		require.Equal(t, [][]interface{}{{int64(4), true}}, g.run(t, "MATCH (n:Doc {k: $k}) RETURN n.seen, n.has_embedding", params).Rows)
	}
}

// TestGetQueryNodeOutsideTransactionReadsFullNode pins why a store outside a
// transaction gives rows the full node unless the statement cannot write: an
// engine's UpdateNode takes the body as written.
func TestGetQueryNodeOutsideTransactionReadsFullNode(t *testing.T) {
	g := newLargeEmbeddingGraph(t, 1)
	node, err := (*StorageExecutor)(nil).getQueryNode(g.store, "doc-0")
	require.NoError(t, err)
	require.Len(t, node.ChunkEmbeddings, 1)
	require.Len(t, node.ChunkEmbeddings[0], 2048)
	node, err = g.exec.getQueryNode(g.store, "doc-0")
	require.NoError(t, err)
	require.Len(t, node.ChunkEmbeddings, 1, "an executor that may write reads the full node")
}

// queryNodeReadSpy counts a store's full node reads and query-node reads.
type queryNodeReadSpy struct {
	storage.Engine
	full, query atomic.Int64
}

func (s *queryNodeReadSpy) GetNode(id storage.NodeID) (*storage.Node, error) {
	s.full.Add(1)
	return s.Engine.GetNode(id)
}

func (s *queryNodeReadSpy) GetQueryNode(id storage.NodeID) (*storage.Node, error) {
	s.query.Add(1)
	return s.Engine.(storage.QueryNodeReader).GetQueryNode(id)
}

// TestAutoCommitQueryNodeReadsLeaveEmbeddingsInStorage: an auto-commit
// statement that cannot write reads the nodes it binds (id seek, traversal,
// relationship endpoint) without their stored vectors, as a transaction does,
// and answers the same rows; n.has_embedding answers from the metadata. A
// statement with a procedure call, which may write outside a transaction,
// reads full nodes.
func TestAutoCommitQueryNodeReadsLeaveEmbeddingsInStorage(t *testing.T) {
	g := newLargeEmbeddingGraph(t, 4)
	spy := &queryNodeReadSpy{Engine: g.store}
	exec := NewStorageExecutorWithQueryCachePolicy(spy, 0, 0)
	run := func(t *testing.T, query string) *ExecuteResult {
		t.Helper()
		result, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err)
		return result
	}
	reads := []string{
		"MATCH (n) WHERE id(n) = 'doc-3' RETURN n.title, n.has_embedding",
		"MATCH (:Hub)-[:HAS]->(n:Doc {k: 3}) RETURN n.title, n.has_embedding",
		"MATCH (:Hub)-[r:HAS]->(:Doc {k: 3}) RETURN endNode(r).title, endNode(r).has_embedding",
	}
	for _, query := range reads {
		t.Run(query, func(t *testing.T) {
			spy.full.Store(0)
			spy.query.Store(0)
			require.Equal(t, [][]interface{}{{"doc 3", true}}, run(t, query).Rows)
			require.Positive(t, spy.query.Load(), "row nodes are read without vectors")
			require.Zero(t, spy.full.Load(), "no row node is read with its vectors")

			g.run(t, "BEGIN", nil)
			inTx := g.run(t, query, nil)
			g.run(t, "ROLLBACK", nil)
			require.Equal(t, inTx.Rows, run(t, query).Rows, "auto-commit answers as a transaction does")
		})
	}
	t.Run("procedure call reads full nodes", func(t *testing.T) {
		spy.full.Store(0)
		spy.query.Store(0)
		result := run(t, "MATCH (:Hub)-[:HAS]->(n:Doc {k: 3}) CALL db.labels() YIELD label RETURN n.title, n.has_embedding LIMIT 1")
		require.Equal(t, [][]interface{}{{"doc 3", true}}, result.Rows)
		require.Zero(t, spy.query.Load())
		require.Positive(t, spy.full.Load())
	})
}

// TestQueryEmbeddingConsumersReadStoredVectors: statements that read the
// vectors themselves (link prediction's semantic score) still read the
// stored ones through the full read, in a transaction too.
func TestQueryEmbeddingConsumersReadStoredVectors(t *testing.T) {
	g := newLargeEmbeddingGraph(t, 3)
	// doc-0 and doc-2 get the same embedding; both link to doc-1.
	same := make([]float32, 2048)
	same[7] = 1
	g.embed(t, "doc-0", same)
	g.embed(t, "doc-2", same)
	g.link(t, "doc-0", "doc-1", "R")
	g.link(t, "doc-1", "doc-2", "R")

	for _, explicit := range []bool{false, true} {
		if explicit {
			g.run(t, "BEGIN", nil)
		}
		result := g.run(t, "CALL gds.linkPrediction.predict.stream({sourceNode: 'doc-0', algorithm: 'common_neighbors'})", nil)
		scores := make(map[string]interface{}, len(result.Rows))
		for _, row := range result.Rows {
			scores[row[1].(string)] = row[4]
		}
		require.Contains(t, scores, "doc-2")
		require.InDelta(t, 1.0, scores["doc-2"], 1e-6)
		if explicit {
			g.run(t, "ROLLBACK", nil)
		}
	}
}

// BenchmarkQueryNodeReadsWithLargeEmbeddings: statements that bind nodes
// carrying a 2048-dimension managed embedding and read or write only their
// properties, by index seek, IN-list seek, traversal and relationship
// endpoint. Each read runs in an explicit transaction (tx-: BEGIN,
// statement, ROLLBACK per iteration) and auto-commit (autocommit-, no
// transaction); the SET is auto-commit, which reads in its implicit
// transaction. Compare B/op and allocs/op: no row reads the vectors.
func BenchmarkQueryNodeReadsWithLargeEmbeddings(b *testing.B) {
	b.Run("warm", func(b *testing.B) { benchmarkQueryNodeReads(b, newLargeEmbeddingGraph(b, 100)) })
	b.Run("cold", func(b *testing.B) { benchmarkQueryNodeReads(b, newColdLargeEmbeddingGraph(b, 100)) })
}

func benchmarkQueryNodeReads(b *testing.B, g *largeEmbeddingGraph) {
	const docs = 100
	ctx := context.Background()
	keys := make([]interface{}, docs)
	for i := range keys {
		keys[i] = int64(i)
	}
	execute := func(b *testing.B, query string, params map[string]interface{}) {
		if _, err := g.exec.Execute(ctx, query, params); err != nil {
			b.Fatal(err)
		}
	}
	for _, query := range []struct {
		name   string
		cypher string
		params map[string]interface{}
	}{
		{"point-seek", "MATCH (n:Doc {k: $k}) RETURN n.title", map[string]interface{}{"k": int64(42)}},
		{"in-list-seek-100", "MATCH (n:Doc) WHERE n.k IN $ks RETURN n.k, n.title", map[string]interface{}{"ks": keys}},
		{"traverse-100", "MATCH (:Hub {k: 0})-[:HAS]->(d:Doc) RETURN d.title", nil},
		{"rel-endpoint-100", "MATCH (:Hub)-[r:HAS]->() RETURN endNode(r).title", nil},
	} {
		b.Run("tx-"+query.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				execute(b, "BEGIN", nil)
				execute(b, query.cypher, query.params)
				execute(b, "ROLLBACK", nil)
			}
		})
		b.Run("autocommit-"+query.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				execute(b, query.cypher, query.params)
			}
		})
	}
	b.Run("autocommit-set-in-list-100", func(b *testing.B) {
		b.ReportAllocs()
		params := map[string]interface{}{"ks": keys}
		for i := 0; i < b.N; i++ {
			params["v"] = int64(i)
			execute(b, "MATCH (n:Doc) WHERE n.k IN $ks SET n.seen = $v", params)
		}
	})
}
