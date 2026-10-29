package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/search/contract"
	"github.com/stretchr/testify/require"
)

func TestDbRetrieveContinuationGeneratedExecutionMatrix(t *testing.T) {
	contract.Run(t, func(t *testing.T, _ int, _ contract.Policy) contract.Fixture {
		ctx := context.Background()
		engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "matrix")
		service := search.NewService(engine, 2, nil)
		t.Cleanup(func() { require.NoError(t, service.Close()) })
		exec := NewStorageExecutor(engine)
		exec.SetSearchService(service)
		return contract.Fixture{Service: service, Engine: engine, Owner: searchContinuationOwner(ctx), Database: GetUseDatabaseFromContext(ctx),
			Pull: func(ctx context.Context, qid string, n int) (contract.Page, error) {
				result, err := exec.Execute(ctx, "CALL db.retrieve($request)", map[string]any{"request": map[string]any{"qid": qid, "n": int64(n)}})
				if err != nil && ctx.Err() != nil {

					return contract.Page{}, ctx.Err()
				}
				if err != nil {
					return contract.Page{}, err
				}
				wire := result.Rows[0][0].(map[string]any)
				page := contract.Page{More: wire["has_more"].(bool), Completion: wire["completion"].(string), Collection: wire["collection_exhausted"].(bool), Ranked: wire["ranked_pool_exhausted"].(bool)}
				for _, row := range wire["results"].([]any) {
					node := row.(map[string]any)["node"].(*storage.Node)
					page.IDs = append(page.IDs, string(node.ID))
				}
				return page, nil
			}}
	})
}
