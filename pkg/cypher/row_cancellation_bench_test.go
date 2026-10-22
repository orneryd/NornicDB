package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"testing"
)

func BenchmarkMaterializedRowOperators(b *testing.B) {
	executor := &StorageExecutor{}
	for _, operator := range []string{"sort", "collect", "validation", "projection"} {
		b.Run(operator, func(b *testing.B) {
			rows := make([]pipelineRow, 2048)
			for index := range rows {
				rows[index] = pipelineRow{"x": int64((index * 7919) % len(rows))}
			}
			input := append([]pipelineRow(nil), rows...)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				switch operator {
				case "sort":
					copy(rows, input)
					if !executor.orderPipelineRowsWithScopes(ctx, rows, rows, []orderByTerm{{column: "x"}}) {
						b.Fatal("sort failed")
					}
				case "collect":
					if _, ok := executor.evaluatePipelineAggregateWithContext(ctx, rows, "collect", "x", false); !ok {
						b.Fatal("collect failed")
					}
				case "projection":
					if _, ok := executor.pipelineApplyWith(ctx, rows, "WITH x AS y"); !ok {
						b.Fatal("projection failed")
					}
				case "validation":
					if err := executor.validatePipelineProjectionValues(rows, "RETURN x", "RETURN"); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// Run both actual accountant configurations; setting an environment variable
// alone does not configure the standalone cypher executor used by benchmarks.
func BenchmarkMaterializedRowQueryMemory(b *testing.B) {
	resetTransactionMemory(b)
	executor := newTransactionMemoryExecutor(b)
	for _, mode := range []string{"disabled", "permissive"} {
		b.Run(mode, func(b *testing.B) {
			var limit int64
			if mode == "permissive" {
				limit = 1 << 40
			}
			ConfigureTransactionMemory(limit, TransactionMemoryLimits{})
			b.ReportAllocs()
			for b.Loop() {
				_, err := executor.Execute(context.Background(), "UNWIND range(1, 2048) AS x WITH x ORDER BY x DESC RETURN size(collect(x)) AS n", nil)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Identical benchmark fixtures also run on each unmodified merge base.
func BenchmarkMaterializedRowKeys(b *testing.B) {
	resetTransactionMemory(b)
	modes := []string{"disabled", "permissive"}
	for _, mode := range modes {
		b.Run(mode, func(b *testing.B) {
			var limit int64
			if mode == "permissive" {
				limit = 1 << 40
			}
			ConfigureTransactionMemory(limit, TransactionMemoryLimits{})
			executor := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(b), "key_bench"), 0, 0)
			for _, tc := range []struct{ name, query string }{
				{"distinct", "UNWIND range(1,2048) AS x RETURN DISTINCT {k:[x,x%7]} AS k"},
				{"grouping", "UNWIND range(1,2048) AS x WITH {k:[x%128,x%7]} AS k RETURN k,count(*)"},
			} {
				b.Run(tc.name, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						if _, err := executor.Execute(context.Background(), tc.query, nil); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
			b.Run("index_keys", func(b *testing.B) {
				value := map[string]interface{}{"k": []interface{}{int64(7), "value", map[string]interface{}{"b": true, "a": int64(2)}}}
				b.ReportAllocs()
				for b.Loop() {
					if key := cypherEquivalenceKey(value); len(key) == 0 {
						b.Fatal("empty key")
					}
				}
			})
		})
	}
}
