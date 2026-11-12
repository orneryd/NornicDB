package cypher

// Read-only row materialization omits stored vector payloads when the statement's
// consumers require only metadata. Native procedures and graph-valued native
// function arguments retain complete images; vector scoring reads its vectors
// independently, and writes preserve stored vectors through transaction staging.

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"math"
	"runtime"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nor413Graph is the reported read shape: documents, each with a current
// version naming a search source and membership in a collection. Every source
// carries `chunks` managed chunk vectors of 2048 dimensions in the embedding
// sidecar, as the embed worker writes them; documents and collections carry
// one each.
type nor413Graph struct {
	engine      *storage.BadgerEngine
	store       storage.Engine
	ids         []interface{}
	vectorBytes uint64
}

const nor413Dims = 2048

func newNOR413Graph(tb testing.TB, documents, chunks int) *nor413Graph {
	tb.Helper()
	engine := newTestMemoryEngine(tb).BadgerEngine
	g := &nor413Graph{engine: engine, store: storage.NewNamespacedEngine(engine, "test")}
	exec := NewStorageExecutor(g.store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	for _, label := range []string{"Document", "Version", "Source", "Collection"} {
		if _, err := exec.Execute(context.Background(), fmt.Sprintf("CREATE INDEX nor413_%s_id FOR (n:%s) ON (n.id)", label, label), nil); err != nil {
			tb.Fatal(err)
		}
	}
	create := func(id, label string, properties map[string]interface{}, vectors int) {
		tb.Helper()
		properties["id"] = id
		if _, err := g.store.CreateNode(tb.Context(), &storage.Node{ID: storage.NodeID(id), Labels: []string{label}, Properties: properties}); err != nil {
			tb.Fatal(err)
		}
		g.embed(tb, storage.NodeID(id), vectors)
	}
	create("col-0", "Collection", map[string]interface{}{"name": "collection"}, 1)
	for i := 0; i < documents; i++ {
		doc, version, source := fmt.Sprintf("doc-%d", i), fmt.Sprintf("ver-%d", i), fmt.Sprintf("src-%d", i)
		create(doc, "Document", map[string]interface{}{"current_version_id": version}, 1)
		create(version, "Version", map[string]interface{}{"current_source_id": source}, 0)
		create(source, "Source", map[string]interface{}{"title": "source " + source}, chunks)
		if err := g.store.CreateEdge(tb.Context(), &storage.Edge{ID: storage.EdgeID(doc + "-member"), StartNode: storage.NodeID(doc), EndNode: "col-0", Type: "MEMBER_OF"}); err != nil {
			tb.Fatal(err)
		}
		g.ids = append(g.ids, doc)
	}
	return g
}

// embed writes count chunk vectors as id's managed embedding through the
// worker sidecar.
func (g *nor413Graph) embed(tb testing.TB, id storage.NodeID, count int) {
	tb.Helper()
	if count == 0 {
		return
	}
	chunks := make([][]float32, count)
	for c := range chunks {
		vector := make([]float32, nor413Dims)
		vector[c%nor413Dims] = 1
		chunks[c] = vector
	}
	writeManagedTestChunks(tb, g.engine, "test:"+id, chunks)
	g.vectorBytes += uint64(count * nor413Dims * 4)
}

const (
	nor413Chain = "UNWIND $ids AS id " +
		"MATCH (d:Document {id: id}) " +
		"OPTIONAL MATCH (v:Version {id: d.current_version_id}) " +
		"WITH d, v, v.current_source_id AS source_id " +
		"OPTIONAL MATCH (s:Source {id: source_id}) "
	nor413WithoutCall   = nor413Chain + "RETURN s.id AS x, s.title AS title"
	nor413CallSubquery  = nor413Chain + "CALL { WITH d OPTIONAL MATCH (d)-[:MEMBER_OF]->(c:Collection) RETURN collect(c.id) AS cs } RETURN s.id AS x, s.title AS title, cs"
	nor413ReadProcedure = nor413Chain + "CALL db.ping() YIELD success RETURN s.id AS x, s.title AS title, success"
)

// nor413Allocated is the fewest bytes one run of statement allocated, over a
// few runs after a warm-up, with before and after outside the measurement.
func nor413Allocated(tb testing.TB, before, statement, after func()) uint64 {
	tb.Helper()
	before()
	statement()
	after()
	least := uint64(math.MaxUint64)
	for i := 0; i < 3; i++ {
		before()
		runtime.GC()
		var start, end runtime.MemStats
		runtime.ReadMemStats(&start)
		statement()
		runtime.ReadMemStats(&end)
		after()
		least = min(least, end.TotalAlloc-start.TotalAlloc)
	}
	return least
}

// TestNOR413AutoCommitCallReadsLeaveVectorsInStorage is the reported
// reproduction: the read chain with a read-only CALL subquery allocates about
// what it allocates without CALL, auto-commit and in an explicit transaction,
// far below the chunk vectors of the nodes it matches, and answers the same
// rows. Before the fix it read every matched node with all its vectors. The bundled
// read procedure declares metadata-only node inputs; native full-image
// consumers have separate vector-preservation controls.
func TestNOR413AutoCommitCallReadsLeaveVectorsInStorage(t *testing.T) {
	g := newNOR413Graph(t, 16, 256)
	exec := NewStorageExecutor(g.store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	params := map[string]interface{}{"ids": g.ids}
	run := func(t *testing.T, query string) *ExecuteResult {
		t.Helper()
		result, err := exec.Execute(context.Background(), query, params)
		require.NoError(t, err)
		return result
	}
	nothing := func() {}
	limit := g.vectorBytes / 4
	for _, tc := range []struct {
		name, query string
		vectorFree  bool
	}{
		{"without-call", nor413WithoutCall, true},
		{"call-subquery", nor413CallSubquery, true},
		{"read-procedure", nor413ReadProcedure, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			autocommit := run(t, tc.query)
			require.Len(t, autocommit.Rows, len(g.ids))
			for i, row := range autocommit.Rows {
				require.Equal(t, fmt.Sprintf("src-%d", i), row[0])
				require.Equal(t, fmt.Sprintf("source src-%d", i), row[1])
			}
			run(t, "BEGIN")
			explicit := run(t, tc.query)
			run(t, "ROLLBACK")
			require.Equal(t, explicit.Rows, autocommit.Rows, "auto-commit answers as a transaction does")

			autoBytes := nor413Allocated(t, nothing, func() { run(t, tc.query) }, nothing)
			txBytes := nor413Allocated(t, func() { run(t, "BEGIN") }, func() { run(t, tc.query) }, func() { run(t, "ROLLBACK") })
			t.Logf("%s: auto-commit %.2f MiB, explicit transaction %.2f MiB, matched chunk vectors %.2f MiB",
				tc.name, float64(autoBytes)/(1<<20), float64(txBytes)/(1<<20), float64(g.vectorBytes)/(1<<20))
			if tc.vectorFree {
				require.Less(t, autoBytes, limit, "an auto-commit read does not load the chunk vectors it never reads")
			} else {
				require.Greater(t, autoBytes, g.vectorBytes/2, "a statement with a procedure call keeps the release head's full read")
			}
			require.Less(t, txBytes, limit, "a read in a transaction does not load the chunk vectors it never reads")
		})
	}
}

// A full read (a procedure or embedded caller) must not turn later eligible
// reads back into per-hit deep copies of the stored vector corpus.
func TestNOR413CallSubqueryAfterFullReadsKeepsAllocationBound(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		g := newNOR413Graph(t, 16, 64)
		exec := NewStorageExecutor(g.store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
		params := map[string]interface{}{"ids": g.ids}
		for i := range g.ids {
			full, err := g.engine.GetNode(t.Context(), storage.NodeID(fmt.Sprintf("test:src-%d", i)))
			require.NoError(t, err)
			require.Len(t, full.ChunkEmbeddings, 64)
		}
		run := func() {
			result, err := exec.Execute(t.Context(), nor413CallSubquery, params)
			require.NoError(t, err)
			require.Len(t, result.Rows, len(g.ids))
			for i, row := range result.Rows {
				require.Equal(t, fmt.Sprintf("src-%d", i), row[0])
			}
		}
		bytes := nor413Allocated(t, func() {}, run, func() {})
		t.Logf("warm CALL-subquery read: %d B, stored vectors: %d B", bytes, g.vectorBytes)
		require.Less(t, bytes, g.vectorBytes/4)
		for i := range g.ids {
			full, err := g.engine.GetNode(t.Context(), storage.NodeID(fmt.Sprintf("test:src-%d", i)))
			require.NoError(t, err)
			require.Len(t, full.ChunkEmbeddings, 64, "the optimization must preserve full reads")
		}
	})
}

// TestNOR413AutoCommitCallReadsUseQueryNodes: the reported statement's row
// nodes, with a CALL subquery, are point-read without their vectors
// (GetQueryNode), never with them (GetNode), as without CALL. With a READ
// procedure declaring metadata inputs, the same read policy applies.
func TestNOR413AutoCommitCallReadsUseQueryNodes(t *testing.T) {
	g := newNOR413Graph(t, 4, 2)
	spy := storagefault.NewQueryNodeReadSpy(g.store)
	exec := NewStorageExecutor(spy, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	for _, tc := range []struct {
		query      string
		vectorFree bool
	}{
		{nor413WithoutCall, true},
		{nor413CallSubquery, true},
		{nor413ReadProcedure, true},
	} {
		t.Run(tc.query, func(t *testing.T) {
			spy.Full.Store(0)
			spy.Query.Store(0)
			result, err := exec.Execute(context.Background(), tc.query, map[string]interface{}{"ids": g.ids})
			require.NoError(t, err)
			require.Len(t, result.Rows, len(g.ids))
			nor413RequireReadMode(t, spy, tc.vectorFree)
		})
	}
}

// nor413RequireReadMode checks that one statement read every node in one
// mode: metadata without vectors (GetQueryNode), or full images (GetNode)
// when an admitted native consumer requires them.
func nor413RequireReadMode(t *testing.T, spy *queryNodeReadSpy, vectorFree bool) {
	t.Helper()
	if vectorFree {
		require.Positive(t, spy.Query.Load(), "row nodes are read without vectors")
		require.Zero(t, spy.Full.Load(), "no node is read with its vectors")
		return
	}
	require.Positive(t, spy.Full.Load(), "row nodes are read in full")
	require.Zero(t, spy.Query.Load(), "no node is read without its vectors")
}

// nor413DocGraph is largeEmbeddingGraph's hub and docs, each doc with a
// property vector (vec) besides its managed 2048-dimension chunk vector.
func newNOR413DocGraph(tb testing.TB, docs int) *largeEmbeddingGraph {
	tb.Helper()
	g := newLargeEmbeddingGraph(tb, 0)
	for i := 0; i < docs; i++ {
		id := storage.NodeID(fmt.Sprintf("doc-%d", i))
		properties := map[string]interface{}{"k": int64(i), "title": fmt.Sprintf("doc %d", i), "vec": []interface{}{float64(i), 1.0}}
		if _, err := g.store.CreateNode(tb.Context(), &storage.Node{ID: id, Labels: []string{"Doc"}, Properties: properties}); err != nil {
			tb.Fatal(err)
		}
		vector := make([]float32, 2048)
		vector[i%len(vector)] = 1
		g.embed(tb, id, vector)
		g.link(tb, "hub", id, "HAS")
	}
	return g
}

// nor413Comparable replaces each node in rows with its identity, labels,
// properties and embedding flag, so rows read by different routes compare by
// content.
func nor413Comparable(rows [][]interface{}) [][]interface{} {
	var value func(interface{}) interface{}
	value = func(v interface{}) interface{} {
		switch typed := v.(type) {
		case *storage.Node:
			return map[string]interface{}{"id": string(typed.ID), "labels": typed.Labels, "properties": typed.Properties, "has_embedding": nodeHasEmbeddingValue(typed)}
		case []interface{}:
			out := make([]interface{}, len(typed))
			for i, item := range typed {
				out[i] = value(item)
			}
			return out
		case map[string]interface{}:
			out := make(map[string]interface{}, len(typed))
			for key, item := range typed {
				out[key] = value(item)
			}
			return out
		}
		return v
	}
	out := make([][]interface{}, len(rows))
	for i, row := range rows {
		out[i] = make([]interface{}, len(row))
		for j, item := range row {
			out[i][j] = value(item)
		}
	}
	return out
}

// TestNOR413VectorReadingCallStatementsKeepResults: auto-commit statements that
// match nodes and read vectors through CALL (vector search scoring the stored
// chunk vectors, link prediction's semantic score, a property vector, a whole
// node returned through a subquery) answer as in an explicit transaction and as
// expected.
func TestNOR413VectorReadingCallStatementsKeepResults(t *testing.T) {
	g := newNOR413DocGraph(t, 4)
	same := make([]float32, 2048)
	same[7] = 1
	g.embed(t, "doc-0", same)
	g.embed(t, "doc-2", same)
	g.link(t, "doc-0", "doc-1", "R")
	g.link(t, "doc-1", "doc-2", "R")
	axis3 := make([]interface{}, 2048)
	for i := range axis3 {
		axis3[i] = 0.0
	}
	axis3[3] = 1.0
	params := map[string]interface{}{"q": axis3, "p": []interface{}{3.0, 1.0}}
	for _, tc := range []struct {
		name  string
		query string
		check func(t *testing.T, rows [][]interface{})
	}{
		{"vector-search", "MATCH (h:Hub {k: 0}) CALL db.index.vector.queryNodes('doc_chunks', 2, $q) YIELD node, score RETURN h.k AS hub, node.title AS title, score", func(t *testing.T, rows [][]interface{}) {
			require.Len(t, rows, 2)
			require.Equal(t, "doc 3", rows[0][1])
			require.InDelta(t, 1.0, rows[0][2], 1e-6, "doc 3's stored chunk vector is the query vector")
		}},
		{"link-prediction", "MATCH (s:Doc {k: 0}) CALL gds.linkPrediction.predict.stream({sourceNode: 'doc-0', algorithm: 'common_neighbors'}) YIELD node2, semantic_score WHERE node2 = 'doc-2' RETURN s.title, node2, semantic_score", func(t *testing.T, rows [][]interface{}) {
			require.Len(t, rows, 1)
			require.InDelta(t, 1.0, rows[0][2], 1e-6, "doc-0 and doc-2 have the same stored chunk vector")
		}},
		{"property-vector", "MATCH (n:Doc {k: 3}) CALL { WITH n RETURN n.vec AS vec, vector.similarity.cosine(n.vec, $p) AS sim } RETURN n.title, vec, sim", func(t *testing.T, rows [][]interface{}) {
			require.Len(t, rows, 1)
			require.Equal(t, []float64{3, 1}, rows[0][1])
			require.InDelta(t, 1.0, rows[0][2], 1e-6)
		}},
		{"whole-node", "MATCH (:Hub)-[:HAS]->(n:Doc {k: 3}) CALL { WITH n RETURN n AS m } RETURN m, m.has_embedding", func(t *testing.T, rows [][]interface{}) {
			require.Len(t, rows, 1)
			node, ok := rows[0][0].(*storage.Node)
			require.True(t, ok, "row value %T", rows[0][0])
			require.Equal(t, "doc 3", node.Properties["title"])
			require.Equal(t, []float64{3, 1}, node.Properties["vec"])
			require.Equal(t, true, rows[0][1])
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			autocommit := g.run(t, tc.query, params)
			tc.check(t, autocommit.Rows)
			g.run(t, "BEGIN", nil)
			explicit := g.run(t, tc.query, params)
			g.run(t, "ROLLBACK", nil)
			tc.check(t, explicit.Rows)
			require.Equal(t, nor413Comparable(explicit.Rows), nor413Comparable(autocommit.Rows))
		})
	}
}

// TestNOR413WritingProceduresKeepStoredVectors: statements whose CALL writes
// (a registered WRITE procedure that writes back the row node it was given,
// apoc.cypher.doIt, a writing CALL subquery) write their change and keep the
// stored chunk vectors, auto-commit and in a transaction.
func TestNOR413WritingProceduresKeepStoredVectors(t *testing.T) {
	cleanupAdmissionProcedure(t, "nor413.touch")
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: "nor413.touch", Mode: ProcedureModeWrite, MinArgs: 2, MaxArgs: 2}, func(ctx context.Context, e *StorageExecutor, _ string, args []interface{}) (*ExecuteResult, error) {
		node, ok := args[0].(*storage.Node)
		if !ok {
			return nil, fmt.Errorf("nor413.touch: node argument is %T", args[0])
		}
		after := storage.CopyNode(node)
		after.Properties["seen"] = args[1]
		if err := e.getStorage(ctx).UpdateNode(ctx, storage.NodeUpdateRequest{Before: storage.CopyNode(node), After: after}); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}))
	g := newLargeEmbeddingGraph(t, 4)
	want := g.storedChunks(t, "doc-3")
	require.NotEmpty(t, want)
	for i, query := range []string{
		"MATCH (n:Doc {k: 3}) CALL nor413.touch(n, $v) RETURN n.title",
		"MATCH (n:Doc {k: 3}) CALL apoc.cypher.doIt('MATCH (m:Doc {k: 3}) SET m.seen = $v RETURN m.seen AS seen', {v: $v}) YIELD value RETURN n.title, value.seen",
		"MATCH (n:Doc {k: 3}) CALL { WITH n SET n.seen = $v } RETURN n.title",
	} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/explicit=%v", query, explicit), func(t *testing.T) {
				v := int64(100*i + 1)
				if explicit {
					v++
					g.run(t, "BEGIN", nil)
				}
				result := g.run(t, query, map[string]interface{}{"v": v})
				if explicit {
					g.run(t, "COMMIT", nil)
				}
				require.Len(t, result.Rows, 1)
				require.Equal(t, "doc 3", result.Rows[0][0])
				require.Equal(t, [][]interface{}{{v, true}}, g.run(t, "MATCH (n:Doc {k: 3}) RETURN n.seen, n.has_embedding", nil).Rows)
				require.Equal(t, want, g.storedChunks(t, "doc-3"), "the stored chunk vectors are kept")
			})
		}
	}
}

// BenchmarkNOR413CallReads is the reported read chain over 16 documents whose
// sources carry 64 chunk vectors of 2048 dimensions, auto-commit without CALL,
// with a CALL subquery and with a READ procedure, and in an explicit
// transaction (BEGIN, statement, ROLLBACK).
func BenchmarkNOR413CallReads(b *testing.B) {
	g := newNOR413Graph(b, 16, 64)
	exec := NewStorageExecutor(g.store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	ctx := context.Background()
	params := map[string]interface{}{"ids": g.ids}
	execute := func(b *testing.B, query string) {
		if _, err := exec.Execute(ctx, query, params); err != nil {
			b.Fatal(err)
		}
	}
	for _, tc := range []struct{ name, query string }{
		{"without-call", nor413WithoutCall},
		{"call-subquery", nor413CallSubquery},
		{"read-procedure", nor413ReadProcedure},
	} {
		b.Run("autocommit-"+tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				execute(b, tc.query)
			}
		})
		b.Run("tx-"+tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				execute(b, "BEGIN")
				execute(b, tc.query)
				execute(b, "ROLLBACK")
			}
		})
	}
}
