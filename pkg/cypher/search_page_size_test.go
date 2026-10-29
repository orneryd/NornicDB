package cypher

import (
	"context"
	"fmt"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSearchContinuationPageSizeCypher(t *testing.T) {
	for _, limit := range []int{500, 2, 600} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
			for i := 0; i < limit+2; i++ {
				_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("page-%04d", i)), Properties: map[string]any{"content": "page document"}})
				require.NoError(t, err)
			}
			options := search.DefaultServiceOptions("test")
			if limit != 500 {
				options.SearchCursorMaxPageSize = limit
			}
			svc := search.NewService(store, 3, &options)
			t.Cleanup(func() { require.NoError(t, svc.Close()) })
			require.NoError(t, svc.BuildIndexes(context.Background()))
			exec := NewStorageExecutor(store)
			exec.SetSearchService(svc)
			for _, procedure := range []string{"db.retrieve", "db.rretrieve"} {
				for _, mode := range []string{"id", "ranked", "ranked_then_id"} {
					t.Run(procedure+"/"+mode, func(t *testing.T) {
						run := func(request map[string]any) (*ExecuteResult, error) {
							return exec.Execute(context.Background(), "CALL "+procedure+"($request)", map[string]any{"request": request})
						}
						assertOversized := func(request map[string]any) {
							_, err := run(request)
							var oversized *resultstream.PageSizeExceededError
							require.ErrorAs(t, err, &oversized)
							require.Equal(t, limit, oversized.MaxPageSize)
							code, _ := nornicerrors.Neo4jStatus(err)
							require.Equal(t, "Neo.ClientError.Statement.ArgumentError", code)
							require.Contains(t, err.Error(), "page_size_exceeded")
						}
						start := map[string]any{"mode": mode, "query": "page", "n": int64(limit + 1), "limit": int64(limit + 2)}
						assertOversized(start)
						start["n"] = int64(1)
						first, err := run(start)
						require.NoError(t, err)
						page := first.Rows[0][0].(map[string]any)
						require.EqualValues(t, limit, page["max_page_size"])
						assertOversized(map[string]any{"qid": page["qid"], "n": int64(limit + 1)})
						exact, err := run(map[string]any{"qid": page["qid"], "n": int64(limit)})
						require.NoError(t, err)
						require.Len(t, exact.Rows[0][0].(map[string]any)["results"], limit)
						start["n"] = int64(limit)
						exact, err = run(start)
						require.NoError(t, err)
						require.Len(t, exact.Rows[0][0].(map[string]any)["results"], limit)
						_, err = run(map[string]any{"qid": "bad-qid", "n": int64(1)})
						require.ErrorIs(t, err, resultstream.ErrInvalidQID)
					})
				}
			}
		})
	}
}
