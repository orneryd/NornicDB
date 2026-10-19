package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkWriteProcedureAtomicity(b *testing.B) {
	for _, route := range []string{"registered", "apoc", "dynamicWrite", "dynamicRead"} {
		b.Run(route, func(b *testing.B) {
			base := storage.NewMemoryEngine()
			b.Cleanup(func() { _ = base.Close() })
			exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(base, "u5_bench"), 0, 0)
			ctx := context.Background()
			const statement = "MERGE (n:U5BenchWrite {id:1}) SET n.value=$value RETURN n.value AS value"
			if _, err := exec.Execute(ctx, statement, map[string]interface{}{"value": 0}); err != nil {
				b.Fatal(err)
			}
			query := "CALL apoc.cypher.doIt($statement,{value:$value}) YIELD value RETURN value.value"
			inner := statement
			if route == "dynamicWrite" || route == "dynamicRead" {
				query = "CALL apoc.cypher.run($statement,{value:$value}) YIELD value RETURN value.value"
				if route == "dynamicRead" {
					inner = "RETURN $value AS value"
				}
			}
			if route == "registered" {
				if err := RegisterUserProcedure(ProcedureSpec{
					Name: "u5tx.benchwrite", Signature: "u5tx.benchwrite(value :: INTEGER) :: (value :: INTEGER)",
					Mode: ProcedureModeWrite, MinArgs: 1, MaxArgs: 1, Returns: []ProcedureColumn{{Name: "value", Type: "INTEGER"}},
				}, func(ctx context.Context, nested *StorageExecutor, _ string, args []interface{}) (*ExecuteResult, error) {
					return nested.Execute(ctx, statement, map[string]interface{}{"value": args[0]})
				}); err != nil {
					b.Fatal(err)
				}
				query = "CALL u5tx.benchwrite($value) YIELD value RETURN value"
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := exec.Execute(ctx, query, map[string]interface{}{"value": i, "statement": inner})
				if err != nil || len(result.Rows) != 1 {
					b.Fatalf("procedure result: %v %v", result, err)
				}
			}
		})
	}
}
