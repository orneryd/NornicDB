package cypher

import (
	"context"
	"testing"
)

// BenchmarkDeleteStatements runs a DETACH DELETE of a connected node and a
// DELETE of an unconnected one, each its own auto-commit transaction: both
// read the relationships connecting the node before deleting it.
func BenchmarkDeleteStatements(b *testing.B) {
	for _, bench := range []struct{ name, setup, statement string }{
		{"detach_connected", "CREATE (h:Hub {id: $id})-[:R]->(:Peer), (h)<-[:R]-(:Peer), (h)-[:R]->(:Peer)", "MATCH (h:Hub {id: $id}) DETACH DELETE h"},
		{"delete_unconnected", "CREATE (:Lone {id: $id})", "MATCH (n:Lone {id: $id}) DELETE n"},
	} {
		b.Run(bench.name, func(b *testing.B) {
			exec, _ := newTestExecutor(b)
			ctx := context.Background()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				params := map[string]interface{}{"id": int64(i)}
				b.StopTimer()
				if _, err := exec.Execute(ctx, bench.setup, params); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				if _, err := exec.Execute(ctx, bench.statement, params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
