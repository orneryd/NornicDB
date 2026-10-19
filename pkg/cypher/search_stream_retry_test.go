package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/streamretrytest"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSearchStreamRetryCypher(t *testing.T) {
	for _, procedure := range []string{"db.retrieve", "db.rretrieve"} {
		t.Run(procedure, func(t *testing.T) {
			streamretrytest.Run(t, func(t *testing.T) streamretrytest.Fixture {
				ctx := context.Background()
				engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "retry")
				service := search.NewService(engine)
				t.Cleanup(func() { require.NoError(t, service.Close()) })
				executor := NewStorageExecutor(engine)
				executor.SetSearchService(service)
				return streamretrytest.Fixture{Service: service, Engine: engine, Owner: searchContinuationOwner(ctx), Database: GetUseDatabaseFromContext(ctx),
					Pull: func(ctx context.Context, qid string, n int) (streamretrytest.Page, error) {
						result, err := executor.Execute(ctx, "CALL "+procedure+"($request)", map[string]any{"request": map[string]any{"qid": qid, "n": int64(n)}})
						if err != nil {
							if ctx.Err() != nil {
								return streamretrytest.Page{}, ctx.Err()
							}
							return streamretrytest.Page{}, err
						}
						wire := result.Rows[0][0].(map[string]any)
						page := streamretrytest.Page{More: wire["has_more"].(bool)}
						for _, row := range wire["results"].([]any) {
							page.IDs = append(page.IDs, string(row.(map[string]any)["node"].(*storage.Node).ID))
						}
						return page, nil
						}}
			})
		})
	}
}
