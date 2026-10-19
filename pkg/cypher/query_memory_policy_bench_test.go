package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Both sides return the same rows. Scoring is active with no suppressing
// binding, so this isolates the cost of obtaining complete visibility inputs.
func BenchmarkQueryMemoryScoredRelationshipReads(b *testing.B) {
	exec := codeIntelReadFixture(b, 128, 2048)
	exec.storage.(*storage.NamespacedEngine).GetInnerEngine().(*storage.BadgerEngine).SetDecayEnabled(true)
	for _, tc := range []struct {
		name, query string
		stream      bool
	}{
		{"labelled_count", "MATCH (:CodeIntel)-[r]->(:CodeIntel) RETURN count(r)", false},
		{"identity", "MATCH ()-[r]->() RETURN startNode(r).id AS a,endNode(r).id AS b,type(r) AS t,r.site AS site,r.props_hash AS hash", false},
		{"streamed_identity", "MATCH ()-[r]->() RETURN startNode(r).id AS a,endNode(r).id AS b,type(r) AS t,r.site AS site,r.props_hash AS hash", true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if tc.stream {
					run := executeStreamed(context.Background(), exec, tc.query, 16)
					rows := 0
					if run.stream.streamed() {
						for {
							batch, done := run.stream.Next(128)
							rows += len(batch)
							if done {
								break
							}
						}
					}
					<-run.stream.Done()
					if run.err != nil || rows != 2048 {
						b.Fatalf("streamed identity: %d rows, %v", rows, run.err)
					}
					continue
				}
				result, err := exec.Execute(context.Background(), tc.query, nil)
				if err != nil {
					b.Fatal(err)
				}
				if tc.name == "labelled_count" {
					if len(result.Rows) != 1 || result.Rows[0][0] != int64(2048) {
						b.Fatal("incorrect count", result.Rows)
					}
				} else if len(result.Rows) != 2048 {
					b.Fatal("incorrect identity row count", len(result.Rows))
				}
			}
		})
	}
}
