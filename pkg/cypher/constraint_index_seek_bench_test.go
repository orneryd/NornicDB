package cypher

// Benchmarks for #875: an equality on a property with a uniqueness
// constraint, inside an explicit transaction, against the same lookup on a
// property with an ordinary index.

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkConstraintIndexSeekInTransaction(b *testing.B) {
	exec := newAsyncStackExecutor(b)
	ctx := context.Background()
	run := func(query string, params map[string]interface{}) {
		if _, err := exec.Execute(ctx, query, params); err != nil {
			b.Fatal(query, err)
		}
	}
	run("CREATE CONSTRAINT rec_id FOR (n:Rec) REQUIRE n.id IS UNIQUE", nil)
	run("CREATE INDEX rec_token FOR (n:Rec) ON (n.token)", nil)
	const nodes = 17000
	for start := 0; start < nodes; start += 2000 {
		run(`UNWIND range($a, $b) AS i CREATE (:Rec {id: 'r' + toString(i), token: 't' + toString(i), body: 'body ' + toString(i)})`,
			map[string]interface{}{"a": int64(start), "b": int64(min(start+2000, nodes) - 1)})
	}
	for _, tc := range []struct{ name, query string }{
		{"constraint", "MATCH (n:Rec {id: $v}) RETURN n.body"},
		{"constraint_where", "MATCH (n:Rec) WHERE n.id = $v RETURN n.body"},
		{"constraint_in", "MATCH (n:Rec) WHERE n.id IN [$v] RETURN n.body"},
		{"index", "MATCH (n:Rec {token: $t}) RETURN n.body"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				k := i % nodes
				run("BEGIN", nil)
				res, err := exec.Execute(ctx, tc.query, map[string]interface{}{"v": fmt.Sprintf("r%d", k), "t": fmt.Sprintf("t%d", k)})
				if err != nil {
					b.Fatal(err)
				}
				run("COMMIT", nil)
				if len(res.Rows) != 1 {
					b.Fatalf("rows = %v", res.Rows)
				}
			}
		})
	}
}
