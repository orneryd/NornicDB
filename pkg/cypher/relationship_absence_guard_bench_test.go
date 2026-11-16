package cypher

import (
	"github.com/orneryd/nornicdb/pkg/storage"
	"testing"
)

// Measure public guard evaluation and transaction lifetime at fixed cardinality.
// Rollback keeps the empty-SET writer probe stable; result rows are checked.
func BenchmarkRelationshipAbsenceGuard(b *testing.B) {
	for _, present := range []bool{false, true} {
		name := "absent"
		if present {
			name = "present"
		}
		b.Run(name, func(b *testing.B) {
			exec, store := newClauseSemanticsBenchmarkExecutor(b)
			anchor := createBenchmarkNode(b, store, "anchor", "GuardSource")
			target := createBenchmarkNode(b, store, "target", "GuardTarget")
			if present {
				if err := store.CreateEdge(b.Context(), &storage.Edge{ID: "link", StartNode: anchor, EndNode: target, Type: "LINK"}); err != nil {
					b.Fatal(err)
				}
			}
			const query = "MATCH (d:GuardSource {id:'anchor'}),(x:GuardTarget {id:'target'}) WHERE NOT EXISTS { MATCH (d)-[:LINK]->(x) } SET d += {}, x += {} RETURN d.id"
			want := 1
			if present {
				want = 0
			}
			run := func(query string) *ExecuteResult {
				r, err := exec.Execute(b.Context(), query, nil)
				if err != nil {
					b.Fatal(err)
				}
				return r
			}
			// Prime admission/structural caches equally before the measured repetitions.
			run("BEGIN")
			r := run(query)
			run("ROLLBACK")
			if len(r.Rows) != want {
				b.Fatalf("guard returned %d rows, want %d", len(r.Rows), want)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				run("BEGIN")
				r := run(query)
				run("ROLLBACK")
				if len(r.Rows) != want {
					b.Fatalf("guard returned %d rows, want %d", len(r.Rows), want)
				}
			}
		})
	}
}
