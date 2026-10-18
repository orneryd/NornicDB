package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Public statements with literal maps, bounded to one node. The CREATE case
// deletes its node in the same transaction; MERGE repeatedly matches its fixture.
func BenchmarkPropertyMaterialization(b *testing.B) {
	for _, tc := range []struct{ name, query, setup string }{
		{"create_literal", "CREATE (n:LiteralBench {id:1,name:'alpha',active:true,tags:['a','b']}) DELETE n", ""},
		{"merge_literal", "MERGE (n:LiteralBench {id:1,name:'alpha',active:true}) RETURN n.id", "CREATE (:LiteralBench {id:1,name:'alpha',active:true})"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "literal_bench"))
			ctx := context.Background()
			if tc.setup != "" {
				if _, err := e.Execute(ctx, tc.setup, nil); err != nil {
					b.Fatal(err)
				}
			}
			for i := 0; i < 3; i++ {
				if _, err := e.Execute(ctx, tc.query, nil); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := e.Execute(ctx, tc.query, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
