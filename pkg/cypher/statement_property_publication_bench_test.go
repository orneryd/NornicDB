package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

var propertyViewOwnershipResultSink *ExecuteResult

// BenchmarkStatementPropertyPublication excludes the outer result cache so it
// measures live expression reuse and row publication, not repeated cache hits.
func BenchmarkStatementPropertyPublication(b *testing.B) {
	for _, shape := range []string{"repeated_property", "node_rows", "plain_stream_rows"} {
		b.Run(shape, func(b *testing.B) {
			exec, engine := reviewPropertyExecutor(b)
			exec.cache = nil
			before, err := engine.GetNode(b.Context(), "review:a")
			if err != nil {
				b.Fatal(err)
			}
			after := storage.CopyNode(before)
			after.Properties["tags"] = []string{"alpha", "beta", "gamma"}
			if err := engine.UpdateNode(b.Context(), storage.NodeUpdateRequest{Before: before, After: after}); err != nil {
				b.Fatal(err)
			}
			query := repeatedPropertyReviewQuery()
			if shape != "repeated_property" {
				query = "MATCH (d:Doc {title:'a'}) UNWIND range(1,100) AS i RETURN d"
			}
			if shape == "plain_stream_rows" {
				engine.SetDecayEnabled(false)
			}
			if shape != "plain_stream_rows" && exec.withStatementPropertyViews(b.Context(), query) == nil {
				b.Fatal("workload must admit statement-local reuse")
			}
			if _, err := exec.Execute(b.Context(), query, nil); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if shape == "plain_stream_rows" {
					run := executeStreamed(b.Context(), exec, query, 1)
					rows, exhausted := run.stream.Next(-1)
					<-run.stream.Done()
					if run.err != nil {
						b.Fatal(run.err)
					}
					if !exhausted || len(rows) != 100 {
						b.Fatalf("incomplete stream: %d rows, exhausted=%v", len(rows), exhausted)
					}
					propertyViewOwnershipResultSink = &ExecuteResult{Rows: rows}
				} else {
					propertyViewOwnershipResultSink, err = exec.Execute(b.Context(), query, nil)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
