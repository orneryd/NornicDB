package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSearchProcedureManagedNodeProperties(t *testing.T) {
	for _, cacheSize := range []int{1, 100} {
		t.Run(fmt.Sprintf("cache-%d", cacheSize), func(t *testing.T) {
			ctx := context.Background()
			base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{InMemory: true, NodeCacheMaxEntries: cacheSize}))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			store := storage.NewNamespacedEngine(base, "test")
			base.SetDecayEnabled(true)
			schema := store.GetSchema()
			require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: "retrieval_age", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 86400 * 365, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
			require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "retrieval_age", ProfileRef: "retrieval_age", TargetLabels: []string{"RetrievalSource"}}))
			exec := NewStorageExecutor(store)
			vector := make([]float32, 2048)
			vector[0] = 1
			for i := 1; i <= 2; i++ {
				id := storage.NodeID(fmt.Sprintf("managed-%d", i))
				_, err := store.CreateNode(&storage.Node{ID: id, Labels: []string{"RetrievalSource"}, Properties: map[string]interface{}{"id": string(id), "title": "retrieval catalogue", "ordinary": int64(42)}})
				require.NoError(t, err)
				node, err := base.GetNode("test:" + id)
				require.NoError(t, err)
				node.ChunkEmbeddings = [][]float32{vector}
				node.EmbedMeta = map[string]interface{}{"has_embedding": true, "chunk_count": 1}
				require.NoError(t, base.UpdateNodeEmbeddingSidecar(node))
			}
			svc := search.NewServiceWithDimensions(store, 2048)
			t.Cleanup(func() { require.NoError(t, svc.Close()) })
			require.NoError(t, svc.BuildIndexes(ctx))
			exec.SetSearchService(svc)
			for _, query := range []string{"source", "retrieval"} {
				for _, proc := range []string{"db.retrieve", "db.rretrieve"} {
					t.Run(proc+"/"+query, func(t *testing.T) {
						result, err := exec.Execute(ctx, "CALL "+proc+"({query:$query, embedding:$vector, types:['RetrievalSource'], minSimilarity:0.01, limit:10}) YIELD node, vector_rank RETURN node.id AS id, vector_rank, labels(node), node.ordinary, node.embedding, node", map[string]interface{}{"query": query, "vector": vector})
						require.NoError(t, err)
						require.Len(t, result.Rows, 2)
						for _, row := range result.Rows {
							require.Contains(t, []string{"managed-1", "managed-2"}, row[0], "row=%#v", row)
							require.Equal(t, []interface{}{"RetrievalSource"}, row[2])
							require.Equal(t, int64(42), row[3])
							require.Nil(t, row[4], "managed sidecar is not a user property")
							node := row[5].(*storage.Node)
							require.Equal(t, int64(42), node.Properties["ordinary"])
							require.Empty(t, node.ChunkEmbeddings, "procedure output must not hydrate managed vectors")
						}
					})
				}
			}
		})
	}
}
