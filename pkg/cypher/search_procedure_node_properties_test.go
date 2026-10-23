package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/search/nodecontract"
	"github.com/stretchr/testify/require"
)

func TestSearchProcedureNodeProperties(t *testing.T) {
	for _, policy := range []bool{false, true} {
		t.Run(fmt.Sprintf("policy=%v", policy), func(t *testing.T) {
			ctx := context.Background()
			base, err := storage.NewBadgerEngineInMemory()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			store := storage.NewNamespacedEngine(base, "test")
			base.SetDecayEnabled(policy)
			if policy {
				schema := store.GetSchema()
				require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: "retrieval_age", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 86400 * 365, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
				require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "retrieval_age", ProfileRef: "retrieval_age", TargetLabels: []string{"RetrievalSource"}}))
			}
			exec := NewStorageExecutor(store)
			params := map[string]any{"vector": nodecontract.Vector()}
			for i := 1; i <= 2; i++ {
				_, err := exec.Execute(ctx, "CREATE (:RetrievalSource {id:$id, bucket:$id, title:'source', ordinary:42, embedding:$vector})", map[string]any{"id": fmt.Sprintf("source-%d", i), "vector": params["vector"]})
				require.NoError(t, err)
			}
			for _, ddl := range []string{
				"CREATE VECTOR INDEX retrieval_vector FOR (n:RetrievalSource) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`:2048, `vector.similarity_function`:'cosine'}}",
				"CREATE FULLTEXT INDEX retrieval_text FOR (n:RetrievalSource) ON EACH [n.title]",
			} {
				_, err := exec.Execute(ctx, ddl, nil)
				require.NoError(t, err)
			}
			service := search.NewServiceWithDimensions(store, 2048)
			t.Cleanup(func() { require.NoError(t, service.Close()) })
			service.EnableRelationshipSearch(nil)
			service.StartRelationshipIndex()
			require.NoError(t, service.BuildIndexes(ctx))
			exec.SetSearchService(service)
			nodecontract.Run(t, func(query string) ([][]any, error) {
				result, err := exec.Execute(ctx, query, params)
				if err != nil {
					return nil, err
				}
				return result.Rows, nil
			})
		})
	}
}
