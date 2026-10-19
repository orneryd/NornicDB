package bolt

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Both sides explicitly discard the delivered continuation so the pre-fix
// parent completes every iteration. RUN/PULL use the actual protocol handlers.
func BenchmarkBoltContinuationDelivery(b *testing.B) {
	for _, kind := range []string{"raw", "json"} {
		b.Run(kind, func(b *testing.B) {
			base := storage.NewMemoryEngine()
			b.Cleanup(func() { _ = base.Close() })
			store := storage.NewNamespacedEngine(base, "nornic")
			for _, id := range []storage.NodeID{"a", "b", "c"} {
				if _, err := store.CreateNode(&storage.Node{ID: id}); err != nil {
					b.Fatal(err)
				}
			}
			svc := search.NewService(store)
			b.Cleanup(func() { _ = svc.Close() })
			registry, err := resultstream.NewRegistry(resultstream.Config{MaxStreams: 1})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(registry.Close)
			svc.SetContinuationRegistry(registry)
			executor := cypher.NewStorageExecutor(store)
			executor.SetSearchService(svc)
			conn := &errorConn{}
			session := newTestSession(conn, &cypherQueryExecutor{executor: executor})
			session.connCtx = context.Background()
			query := "CALL db.retrieve({mode:'id',n:1})"
			if kind == "json" {
				query += " YIELD page RETURN apoc.convert.toJson(page) AS page"
			}
			run := buildRunMessageData(query, nil, nil)
			pull := encodePackStreamMap(map[string]any{"n": int64(-1)})
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				conn.writeData = conn.writeData[:0]
				if err := session.handleRun(run); err != nil {
					b.Fatal(err)
				}
				if session.lastResult == nil || len(session.lastResult.Rows) != 1 {
					b.Fatal("RUN did not produce its page")
				}
				var qid string
				if kind == "raw" {
					qid = session.lastResult.Rows[0][0].(map[string]any)["qid"].(string)
				} else {
					var page struct {
						QID string `json:"qid"`
					}
					if err := json.Unmarshal([]byte(session.lastResult.Rows[0][0].(string)), &page); err != nil {
						b.Fatal(err)
					}
					qid = page.QID
				}
				if err := session.handlePull(pull); err != nil {
					b.Fatal(err)
				}
				if _, err := svc.SearchTextContinuation(context.Background(), "", nil, search.SearchContinuationRequest{Owner: "embedded", QID: qid, Discard: true}, nil, nil, nil, search.ChunkedSearchErrorPolicy{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
