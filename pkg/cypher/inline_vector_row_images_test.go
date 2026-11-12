package cypher

import (
	"context"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Storage metadata reads may retain inline vectors, while vector scoring's
// output may omit them. Collection membership must compare entity identity,
// never payload layout or private read provenance. Native consumers still
// receive the complete image.
func newVectorImageIdentityGraph(t *testing.T, mode string) *largeEmbeddingGraph {
	t.Helper()
	engine := newTestMemoryEngine(t).BadgerEngine
	if mode == "write-behind" {
		var err error
		engine, err = fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
			DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour,
		}))
		require.NoError(t, err)
		fixtureowner.Own(t, engine)
	}
	var store storage.Engine = storage.NewNamespacedEngine(engine, "test")
	var staged *storage.BadgerTransaction
	if mode == "pending" || mode == "write-behind" {
		var err error
		staged, err = engine.BeginTransaction(t.Context())
		require.NoError(t, err)
		t.Cleanup(func() { _ = staged.Rollback() })
		require.NoError(t, staged.SetNamespace("test"))
		if mode == "write-behind" {
			require.NoError(t, staged.SetImplicit(true))
		}
		store = &transactionStorageWrapper{tx: staged, underlying: engine, namespace: "test", separator: ":", mutatedNodeIDs: make(map[string]struct{})}
	}
	_, err := store.CreateNode(t.Context(), &storage.Node{ID: "hub", Labels: []string{"Hub"}})
	require.NoError(t, err)
	vector := make([]float32, 2048)
	vector[3] = 1
	doc := &storage.Node{ID: "doc-3", Labels: []string{"Doc"}, Properties: map[string]interface{}{"k": int64(3), "title": "doc 3"},
		ChunkEmbeddings: [][]float32{vector}, EmbedMeta: map[string]interface{}{"has_embedding": true, "chunk_count": 1}}
	if mode == "sidecar" {
		doc.ChunkEmbeddings = nil
	}
	_, err = store.CreateNode(t.Context(), doc)
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(t.Context(), &storage.Edge{ID: "link", StartNode: "hub", EndNode: "doc-3", Type: "HAS"}))
	if mode == "sidecar" {
		writeManagedTestChunks(t, engine, "test:doc-3", [][]float32{vector})
	}
	if mode == "write-behind" {
		require.NoError(t, staged.Commit())
		store = storage.NewNamespacedEngine(engine, "test")
	}
	return &largeEmbeddingGraph{engine: engine, store: store, exec: NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})}
}

func TestVectorSearchNodeIdentityAcrossReadImages(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		nor413CallLookup(t)
		const name = "image.identityVector"
		cleanupAdmissionProcedure(t, name)
		require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: name, Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 1,
			Returns: []ProcedureColumn{{Name: "component", Type: "FLOAT"}}},
			func(_ context.Context, _ *StorageExecutor, _ string, args []interface{}) (*ExecuteResult, error) {
				return &ExecuteResult{Columns: []string{"component"}, Rows: [][]interface{}{{nor413VectorComponent(args[0])}}}, nil
			}))
		for _, mode := range []string{"sidecar", "inline", "pending", "write-behind"} {
			t.Run(mode, func(t *testing.T) {
				graph := newVectorImageIdentityGraph(t, mode)
				vector := make([]float64, 2048)
				vector[3] = 1
				params := map[string]interface{}{"q": vector}
				const head = "MATCH (:Hub)-[:HAS]->(n:Doc {k:3}) CALL db.index.vector.queryNodes('doc_chunks',1,$q) YIELD node AS m,score "
				for _, full := range []bool{false, true} {
					query := head
					if full {
						query += "CALL image.identityVector(m) YIELD component "
					}
					query += "RETURN id(n)=id(m),apoc.coll.contains([n],m),score,n,m"
					if full {
						query += ",component"
					}
					result := graph.run(t, query, params)
					require.Len(t, result.Rows, 1)
					row := result.Rows[0]
					require.Equal(t, true, row[0])
					require.Equal(t, true, row[1], "same entity remains a member across payload/provenance differences")
					require.InDelta(t, 1.0, row[2], 1e-9)
					matched, ok := row[3].(*storage.Node)
					require.True(t, ok)
					searched, ok := row[4].(*storage.Node)
					require.True(t, ok)
					require.Equal(t, "doc 3", matched.Properties["title"])
					require.Equal(t, matched.Properties, searched.Properties)
					require.Equal(t, matched.Identity(), searched.Identity())
					if full {
						require.Len(t, matched.ChunkEmbeddings, 1)
						require.Len(t, searched.ChunkEmbeddings, 1)
						require.Equal(t, float64(1), row[5])
					} else {
						resident := 1
						if mode == "sidecar" {
							resident = 0
						}
						require.Len(t, matched.ChunkEmbeddings, resident, "storage may retain resident vectors")
						require.Empty(t, searched.ChunkEmbeddings, "metadata scoring output does not retain vectors")
					}
					stored, err := graph.store.GetNode(t.Context(), "doc-3")
					require.NoError(t, err)
					require.Len(t, stored.ChunkEmbeddings, 1, "query projection never changes retained vectors")
					require.Equal(t, float32(1), stored.ChunkEmbeddings[0][3])
				}
			})
		}
	})
}
