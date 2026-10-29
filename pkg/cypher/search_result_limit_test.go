package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/search/contract"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDbRetrieveContinuationResultLimitPublic(t *testing.T) {
	contract.RunResultLimit(t, func(t *testing.T, vector bool) contract.LimitFixture {
		engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "ceiling")
		service := search.NewService(engine, 2, nil)
		t.Cleanup(func() { require.NoError(t, service.Close()) })
		exec := NewStorageExecutor(engine)
		exec.SetSearchService(service)
		request := func(ctx context.Context, req map[string]any) (contract.Page, error) {
			statement := "CALL db.retrieve($request)"
			_, paged := req["n"]
			if !paged {
				statement += " YIELD node RETURN node"
			}
			result, err := exec.Execute(ctx, statement, map[string]any{"request": req})
			if err != nil {
				return contract.Page{}, err
			}
			if !paged {
				var page contract.Page
				for _, row := range result.Rows {
					page.IDs = append(page.IDs, string(row[0].(*storage.Node).ID))
				}
				return page, nil
			}
			wire := result.Rows[0][0].(map[string]any)
			page := contract.Page{More: wire["has_more"].(bool), Completion: wire["completion"].(string), Collection: wire["collection_exhausted"].(bool), Ranked: wire["ranked_pool_exhausted"].(bool)}
			page.QID, _ = wire["qid"].(string)
			for _, row := range wire["results"].([]any) {
				node := row.(map[string]any)["node"].(*storage.Node)
				page.IDs = append(page.IDs, string(node.ID))
			}
			return page, nil
		}
		return contract.LimitFixture{Fixture: contract.Fixture{Service: service, Engine: engine, Pull: func(ctx context.Context, qid string, n int) (contract.Page, error) {
			return request(ctx, map[string]any{"qid": qid, "n": int64(n)})
		}}, Start: func(ctx context.Context, r contract.LimitRequest) (contract.Page, error) {
			req := map[string]any{"query": "ceilingtoken", "limit": int64(r.Limit)}
			if r.N > 0 {
				req["n"], req["max_results"], req["mode"] = int64(r.N), int64(r.MaxResults), string(r.Mode)
			}
			if vector {
				req["embedding"] = []float64{1, 0}
			}
			return request(ctx, req)
		}}
	})
}
