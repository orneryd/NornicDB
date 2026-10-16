package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestVectorSettersPreserveEmbeddingsAfterRestart(t *testing.T) {
	for _, layout := range []string{"properties", "inline-chunks", "separate-chunks", "sidecar", "named", "empty-property", "empty-sidecar", "failed-inline", "failed-sidecar"} {
		for _, mode := range []string{"direct", "implicit", "commit", "rollback"} {
			for _, byID := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/id=%v", layout, mode, byID), func(t *testing.T) {
					dir := t.TempDir()
					open := func() *storage.BadgerEngine {
						engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, LowMemory: true})
						require.NoError(t, err)
						engine.SetEmbeddingTextPolicy(storage.EmbeddingTextPolicy{Include: []string{"text"}})
						return engine
					}
					raw := open()
					t.Cleanup(func() { require.NoError(t, raw.Close()) })
					store := storage.NewNamespacedEngine(raw, "vector_restart")
					old := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
					empty := layout == "empty-property" || layout == "empty-sidecar" || layout == "failed-inline" || layout == "failed-sidecar"
					initialVector := []float64{1, 2}
					vectors := []string{"[1.0,2.0]", "[3.0,4.0]", "[3.0,4.0]", "[5.0,6.0]"}
					wantVectors := [][]float64{{1, 2}, {3, 4}, {3, 4}, {5, 6}}
					if empty {
						initialVector = []float64{}
						vectors = []string{"[]", "[3.0,4.0]", "[3.0,4.0]", "[]"}
						wantVectors = [][]float64{{}, {3, 4}, {3, 4}, {}}
					}
					node := &storage.Node{ID: "a", Labels: []string{"Clock"}, Properties: map[string]any{"text": "source", "emb": initialVector}, CreatedAt: old, UpdatedAt: old}
					var metadata map[string]any
					if layout == "empty-sidecar" {
						metadata = map[string]any{"chunk_count": 0, "has_embedding": false}
					}
					if layout == "failed-inline" || layout == "failed-sidecar" {
						metadata = map[string]any{"chunk_count": 0, "has_embedding": false, "embedding_failed": true, "embedding_error": "permanent failure"}
					}
					if layout == "failed-inline" {
						node.EmbedMeta = metadata
					}
					var chunks [][]float32
					if layout == "inline-chunks" || layout == "separate-chunks" || layout == "sidecar" {
						chunks = [][]float32{{0.25, 0.5}, {0.75, 1}}
						if layout == "separate-chunks" {
							chunks = make([][]float32, 4)
							for i := range chunks {
								chunks[i] = make([]float32, 4000)
								for j := range chunks[i] {
									chunks[i][j] = float32(j) / 4000
								}
							}
						}
						if layout != "sidecar" {
							node.ChunkEmbeddings = chunks
							node.EmbedMeta = map[string]any{"chunk_count": len(chunks), "has_embedding": true}
						}
					}
					if layout == "named" {
						node.NamedEmbeddings = map[string][]float32{"managed": {0.25, 0.5}}
					}
					_, err := store.CreateNode(node)
					require.NoError(t, err)
					_, err = store.CreateNode(&storage.Node{ID: "b", Properties: map[string]any{}, CreatedAt: old, UpdatedAt: old})
					require.NoError(t, err)
					require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "CLOCK", Properties: map[string]any{"emb": initialVector}, CreatedAt: old, UpdatedAt: old}))
					if layout == "sidecar" || layout == "empty-sidecar" || layout == "failed-sidecar" {
						payload, err := store.GetNode("a")
						require.NoError(t, err)
						payload.ChunkEmbeddings = chunks
						if layout == "sidecar" {
							metadata = map[string]any{"chunk_count": len(chunks), "has_embedding": true}
						}
						payload.EmbedMeta = metadata
						require.NoError(t, store.UpdateNodeEmbeddingSidecar(payload))
					}
					initial, err := store.GetNode("a")
					require.NoError(t, err)
					require.Equal(t, chunks, initial.ChunkEmbeddings)
					for key, value := range metadata {
						require.Contains(t, initial.EmbedMeta, key)
						require.EqualValues(t, value, initial.EmbedMeta[key], key)
					}
					var engine storage.Engine = store
					if mode == "direct" {
						engine = &nonTransactionalEngine{Engine: store}
					}
					executor := NewStorageExecutor(engine)
					ctx := context.Background()
					t.Cleanup(func() { _, _ = executor.Execute(ctx, "ROLLBACK", nil) })
					explicit := mode == "commit" || mode == "rollback"
					if explicit {
						_, err = executor.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}
					priorNode, priorEdge := old, old
					for step, vector := range vectors {
						np, na, ns := "MATCH (n:Clock) ", "n", " RETURN n"
						ep, ea, es := "MATCH ()-[r:CLOCK]->() ", "r", " RETURN r"
						if byID {
							np, na, ns, ep, ea, es = "", "'a'", "", "", "'e'", ""
						}
						before := time.Now()
						nodeWrite, err := executor.Execute(ctx, np+"CALL db.create.setNodeVectorProperty("+na+",'emb',"+vector+")"+ns, nil)
						require.NoError(t, err)
						edgeWrite, err := executor.Execute(ctx, ep+"CALL db.create.setRelationshipVectorProperty("+ea+",'emb',"+vector+")"+es, nil)
						require.NoError(t, err)
						after := time.Now()
						nr, err := executor.Execute(ctx, "MATCH (n:Clock) RETURN n", nil)
						require.NoError(t, err)
						er, err := executor.Execute(ctx, "MATCH ()-[r:CLOCK]->() RETURN r", nil)
						require.NoError(t, err)
						require.Len(t, nr.Rows, 1)
						require.Len(t, er.Rows, 1)
						n, r := nr.Rows[0][0].(*storage.Node), er.Rows[0][0].(*storage.Edge)
						if step == 1 || step == 3 {
							require.True(t, n.UpdatedAt.After(priorNode))
							require.True(t, r.UpdatedAt.After(priorEdge))
							require.False(t, n.UpdatedAt.Before(before))
							require.False(t, r.UpdatedAt.Before(before))
							require.False(t, n.UpdatedAt.After(after))
							require.False(t, r.UpdatedAt.After(after))
						} else {
							require.True(t, n.UpdatedAt.Equal(priorNode), "node no-op preserves time")
							require.True(t, r.UpdatedAt.Equal(priorEdge), "relationship no-op preserves time")
						}
						require.True(t, n.CreatedAt.Equal(old))
						require.True(t, r.CreatedAt.Equal(old))
						if !byID {
							require.Len(t, nodeWrite.Rows, 1)
							require.Len(t, edgeWrite.Rows, 1)
							require.True(t, nodeWrite.Rows[0][0].(*storage.Node).UpdatedAt.Equal(n.UpdatedAt))
							require.True(t, edgeWrite.Rows[0][0].(*storage.Edge).UpdatedAt.Equal(r.UpdatedAt))
						}
						for _, properties := range []map[string]any{n.Properties, r.Properties} {
							value, ok := toFloat64Slice(properties["emb"])
							require.True(t, ok)
							require.Equal(t, wantVectors[step], value)
						}
						require.Equal(t, chunks, n.ChunkEmbeddings)
						require.Len(t, n.EmbedMeta, len(initial.EmbedMeta))
						for key, value := range initial.EmbedMeta {
							require.Contains(t, n.EmbedMeta, key)
							require.EqualValues(t, value, n.EmbedMeta[key], key)
						}
						priorNode, priorEdge = n.UpdatedAt, r.UpdatedAt
					}
					if explicit {
						_, err = executor.Execute(ctx, map[string]string{"commit": "COMMIT", "rollback": "ROLLBACK"}[mode], nil)
						require.NoError(t, err)
					}
					require.NoError(t, raw.Close())
					raw = open()
					store = storage.NewNamespacedEngine(raw, "vector_restart")
					cold, err := store.GetNode("a")
					require.NoError(t, err)
					edge, err := store.GetEdge("e")
					require.NoError(t, err)
					wantVector := wantVectors[len(wantVectors)-1]
					if mode == "rollback" {
						priorNode, priorEdge, wantVector = old, old, initialVector
					}
					require.True(t, cold.UpdatedAt.Equal(priorNode))
					require.True(t, edge.UpdatedAt.Equal(priorEdge))
					require.True(t, cold.CreatedAt.Equal(old))
					require.True(t, edge.CreatedAt.Equal(old))
					for _, properties := range []map[string]any{cold.Properties, edge.Properties} {
						value, ok := toFloat64Slice(properties["emb"])
						require.True(t, ok)
						require.Equal(t, wantVector, value)
					}
					require.Equal(t, "source", cold.Properties["text"])
					require.Equal(t, chunks, cold.ChunkEmbeddings, "unchanged source keeps its vectors after restart")
					require.Len(t, cold.EmbedMeta, len(initial.EmbedMeta))
					for key, value := range initial.EmbedMeta {
						require.Contains(t, cold.EmbedMeta, key)
						// The binary codec may narrow an integer's Go type.
						require.EqualValues(t, value, cold.EmbedMeta[key], key)
					}
					require.Equal(t, initial.NamedEmbeddings, cold.NamedEmbeddings)
				})
			}
		}
	}
}
