package bolt

// Metadata consumers over Bolt avoid unused stored vectors, while native
// consumers retain complete node images and writes preserve stored vectors.

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"encoding/json"
	"fmt"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/apoc/registry"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newNOR413BoltGraph is a hub linked to four docs, each with a property
// vector (vec) and a managed 2048-dimension chunk vector in the embedding
// sidecar, as the embed worker writes it.
func newNOR413BoltGraph(t *testing.T) (*storage.BadgerEngine, storage.Engine) {
	t.Helper()
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	// "seen" doesn't feed the embedding text, so setting it keeps the embedding.
	engine.SetEmbeddingTextPolicy(storage.EmbeddingTextPolicy{Exclude: []string{"seen"}, IncludeLabels: true})
	store := storage.NewNamespacedEngine(engine, "test")
	_, err = cypher.NewStorageExecutor(store).Execute(context.Background(), "CREATE INDEX nor413_doc_k FOR (n:Doc) ON (n.k)", nil)
	require.NoError(t, err)
	_, err = store.CreateNode(t.Context(), &storage.Node{ID: "hub", Labels: []string{"Hub"}, Properties: map[string]interface{}{"k": int64(0)}})
	require.NoError(t, err)
	for i := 0; i < 4; i++ {
		id := storage.NodeID(fmt.Sprintf("doc-%d", i))
		_, err := store.CreateNode(t.Context(), &storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]interface{}{"k": int64(i), "title": fmt.Sprintf("doc %d", i), "vec": []interface{}{float64(i), 1.0}}})
		require.NoError(t, err)
		stored, err := engine.GetNode(t.Context(), "test:"+id)
		require.NoError(t, err)
		payload := storage.CopyNode(stored)
		vector := make([]float32, 2048)
		vector[i] = 1
		payload.ChunkEmbeddings = [][]float32{vector}
		payload.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": 1}
		require.NoError(t, engine.UpdateNodeEmbeddingSidecar(t.Context(), payload))
		require.NoError(t, store.CreateEdge(t.Context(), &storage.Edge{ID: storage.EdgeID("hub-" + string(id)), StartNode: "hub", EndNode: id, Type: "HAS"}))
	}
	return engine, store
}

func nor413BoltDriver(t *testing.T, port int) neo4j.DriverWithContext {
	t.Helper()
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	return driver
}

// nor413BoltRun runs query auto-commit, or in an explicit transaction that it
// commits, and returns each record's values as JSON.
func nor413BoltRun(t *testing.T, driver neo4j.DriverWithContext, explicit bool, query string, params map[string]interface{}) []string {
	t.Helper()
	ctx := context.Background()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	collect := func(result neo4j.ResultWithContext) []string {
		var rows []string
		for result.Next(ctx) {
			encoded, err := json.Marshal(result.Record().Values)
			require.NoError(t, err)
			rows = append(rows, string(encoded))
		}
		require.NoError(t, result.Err())
		return rows
	}
	if !explicit {
		result, err := session.Run(ctx, query, params)
		require.NoError(t, err)
		return collect(result)
	}
	tx, err := session.BeginTransaction(ctx)
	require.NoError(t, err)
	result, err := tx.Run(ctx, query, params)
	require.NoError(t, err)
	rows := collect(result)
	require.NoError(t, tx.Commit(ctx))
	return rows
}

// TestNOR413BoltAutoCommitCallReadsUseQueryNodes: through Bolt, auto-commit
// reads with a read-only CALL subquery point-read their row nodes without
// vectors, never with them, as the same read without CALL. With a READ
// procedure declaring metadata inputs the same policy applies; a native
// vector-reading function instead receives the complete image.
func TestNOR413BoltAutoCommitCallReadsUseQueryNodes(t *testing.T) {
	handler, err := registry.Compile(func(node *storage.Node) int { return len(node.ChunkEmbeddings) }, registry.OpaqueReads)
	require.NoError(t, err)
	previous := cypher.PluginFunctionLookup
	t.Cleanup(func() { cypher.PluginFunctionLookup = previous })
	cypher.PluginFunctionLookup = func(name string) (*cypher.PluginFunctionHandler, bool) {
		if name == "images.vectorCount" {
			return handler, true
		}
		if previous != nil {
			return previous(name)
		}
		return nil, false
	}
	_, store := newNOR413BoltGraph(t)
	spy := storagefault.NewQueryNodeReadSpy(store)
	_, port := startBoltIntegrationServer(t, spy, boltIntegrationAutocommit)
	driver := nor413BoltDriver(t, port)
	for _, tc := range []struct {
		query      string
		vectorFree bool
		want       string
	}{
		{"MATCH (:Hub)-[:HAS]->(d:Doc {k: 3}) RETURN d.title, d.has_embedding", true, `["doc 3",true]`},
		{"MATCH (h:Hub) CALL { WITH h MATCH (h)-[:HAS]->(d:Doc {k: 3}) RETURN d } RETURN d.title, d.has_embedding", true, `["doc 3",true]`},
		{"MATCH (:Hub)-[:HAS]->(d:Doc {k: 3}) CALL db.labels() YIELD label WHERE label = 'Doc' RETURN d.title, d.has_embedding", true, `["doc 3",true]`},
		{"MATCH (:Hub)-[:HAS]->(d:Doc {k: 3}) RETURN d.title, d.has_embedding, images.vectorCount(d)", false, `["doc 3",true,1]`},
	} {
		t.Run(tc.query, func(t *testing.T) {
			spy.Full.Store(0)
			spy.Query.Store(0)
			require.Equal(t, []string{tc.want}, nor413BoltRun(t, driver, false, tc.query, nil))
			if tc.vectorFree {
				require.Positive(t, spy.Query.Load(), "row nodes are read without vectors")
				require.Zero(t, spy.Full.Load(), "no row node is read with its vectors")
			} else {
				require.Positive(t, spy.Full.Load(), "row nodes are read in full")
				require.Zero(t, spy.Query.Load(), "no row node is read without its vectors")
			}
		})
	}
}

// TestNOR413BoltVectorReadingCallStatementsKeepResults: through Bolt, statements
// that match nodes and read vectors through CALL (vector search scoring stored
// chunk vectors, a property vector, a whole node returned through a subquery)
// answer auto-commit as in an explicit transaction and as expected; writing
// CALLs write their change and keep the stored chunk vectors.
func TestNOR413BoltVectorReadingCallStatementsKeepResults(t *testing.T) {
	engine, store := newNOR413BoltGraph(t)
	_, port := startBoltIntegrationServer(t, store, boltIntegrationExplicitTx)
	driver := nor413BoltDriver(t, port)
	axis3 := make([]interface{}, 2048)
	for i := range axis3 {
		axis3[i] = 0.0
	}
	axis3[3] = 1.0
	params := map[string]interface{}{"q": axis3, "p": []interface{}{3.0, 1.0}}
	for _, tc := range []struct{ name, query, want string }{
		{"vector-search", "MATCH (h:Hub {k: 0}) CALL db.index.vector.queryNodes('doc_chunks', 1, $q) YIELD node, score RETURN h.k, node.title, score > 0.999999", `[0,"doc 3",true]`},
		{"property-vector", "MATCH (n:Doc {k: 3}) CALL { WITH n RETURN n.vec AS vec, vector.similarity.cosine(n.vec, $p) AS sim } RETURN n.title, vec, sim > 0.999999", `["doc 3",[3,1],true]`},
		{"whole-node", "MATCH (:Hub)-[:HAS]->(n:Doc {k: 3}) CALL { WITH n RETURN n AS m } RETURN m.title, m.vec, m.has_embedding, properties(m) = properties(n)", `["doc 3",[3,1],true,true]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			autocommit := nor413BoltRun(t, driver, false, tc.query, params)
			require.Equal(t, []string{tc.want}, autocommit)
			require.Equal(t, autocommit, nor413BoltRun(t, driver, true, tc.query, params), "auto-commit answers as a transaction does")
		})
	}
	t.Run("whole-node-value", func(t *testing.T) {
		query := "MATCH (:Hub)-[:HAS]->(n:Doc {k: 3}) CALL { WITH n RETURN n AS m } RETURN m"
		autocommit := nor413BoltRun(t, driver, false, query, nil)
		require.Len(t, autocommit, 1)
		require.Equal(t, autocommit, nor413BoltRun(t, driver, true, query, nil))
	})

	stored := func(t *testing.T) [][]float32 {
		t.Helper()
		node, err := engine.GetNode(t.Context(), "test:doc-3")
		require.NoError(t, err)
		return node.ChunkEmbeddings
	}
	want := stored(t)
	require.Len(t, want, 1)
	for i, query := range []string{
		"MATCH (n:Doc {k: 3}) CALL { WITH n SET n.seen = $v } RETURN n.title",
		"MATCH (n:Doc {k: 3}) CALL apoc.cypher.doIt('MATCH (m:Doc {k: 3}) SET m.seen = $v RETURN m.seen AS seen', {v: $v}) YIELD value RETURN n.title",
	} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("write/%s/explicit=%v", query, explicit), func(t *testing.T) {
				v := int64(10*i + 1)
				if explicit {
					v++
				}
				require.Equal(t, []string{`["doc 3"]`}, nor413BoltRun(t, driver, explicit, query, map[string]interface{}{"v": v}))
				require.Equal(t, []string{fmt.Sprintf(`[%d,true]`, v)}, nor413BoltRun(t, driver, false, "MATCH (n:Doc {k: 3}) RETURN n.seen, n.has_embedding", nil))
				require.Equal(t, want, stored(t), "the stored chunk vectors are kept")
			})
		}
	}
}
