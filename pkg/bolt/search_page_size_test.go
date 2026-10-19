package bolt

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSearchContinuationPageSizeBolt(t *testing.T) {
	for _, limit := range []int{500, 2, 600} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			base := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			store := storage.NewNamespacedEngine(base, "nornic")
			for i := 0; i < limit + 2; i++ {
				_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("page-%04d", i))})
				require.NoError(t, err)
			}
			server, port := startBoltIntegrationServer(t, store)
			svc := search.NewService(store)
			t.Cleanup(func() { require.NoError(t, svc.Close()) })
			maxPageSize := limit
			if limit == 500 {
				maxPageSize = 0 // Registry default.
			}
			registry, err := resultstream.NewRegistry(resultstream.Config{MaxPageSize: maxPageSize})
			require.NoError(t, err)
			t.Cleanup(registry.Close)
			svc.SetContinuationRegistry(registry)
			server.executor.(*cypherQueryExecutor).executor.SetSearchService(svc)
			conn := openBoltTestConn(t, port)
			failure := func(request map[string]any) map[string]any {
				require.NoError(t, SendRun(t, conn, "CALL db.retrieve($request)", map[string]any{"request": request}, nil))
				kind, data, err := ReadMessage(conn)
				require.NoError(t, err)
				require.Equal(t, byte(MsgFailure), kind)
				body, _, err := decodePackStreamMap(data, 0)
				require.NoError(t, err)
				require.NoError(t, SendReset(t, conn))
				_, err = AssertSuccess(t, conn)
				require.NoError(t, err)
				return body
			}
			assertOversized := func(request map[string]any) {
				body := failure(request)
				require.Equal(t, "Neo.ClientError.Statement.ArgumentError", body["code"])
				require.Equal(t, "page_size_exceeded", body["request_status"])
				require.EqualValues(t, limit, body["max_page_size"])
			}
			assertOversized(map[string]any{"mode": "id", "n": int64(limit + 1)})
			start := func(n int) map[string]any {
				require.NoError(t, SendRun(t, conn, "CALL db.retrieve($request)", map[string]any{"request": map[string]any{"mode": "id", "n": int64(n)}}, nil))
				_, err := AssertSuccess(t, conn)
				require.NoError(t, err)
				records, _ := pullStatement(t, conn, map[string]any{"n": int64(-1)})
				return records[0][0].(map[string]any)
			}
			first := start(1)
			require.EqualValues(t, limit, first["max_page_size"])
			assertOversized(map[string]any{"qid": first["qid"], "n": int64(limit + 1)})
			require.NoError(t, SendRun(t, conn, "CALL db.retrieve($request)", map[string]any{"request": map[string]any{"qid": first["qid"], "n": int64(limit)}}, nil))
			_, err = AssertSuccess(t, conn)
			require.NoError(t, err)
			records, _ := pullStatement(t, conn, map[string]any{"n": int64(-1)})
			require.Len(t, records[0][0].(map[string]any)["results"], limit)
			require.Len(t, start(limit)["results"], limit)
			bad := failure(map[string]any{"qid": "bad-qid", "n": int64(1)})
			require.NotContains(t, bad, "max_page_size")
			require.NotContains(t, bad["message"], "page_size_exceeded")
		})
	}
}
