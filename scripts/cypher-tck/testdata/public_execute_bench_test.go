package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkExecute_ScalarParameter owns both scalar execution workloads. The
// identical fixture is installed into baseline and current checkouts, so setup
// uses their shared public constructor and supported cache configuration.
func BenchmarkExecute_ScalarParameter(b *testing.B) {
	for _, mode := range []struct {
		name   string
		public bool
	}{{"Internal", false}, {"Public", true}} {
		b.Run(mode.name, func(b *testing.B) {
			var exec *StorageExecutor
			if mode.public {
				b.Setenv("NORNICDB_QUERY_CACHE_ENABLED", "false")
				store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "allocation-execute")
				exec = NewStorageExecutor(store)
				if entries, _ := exec.QueryCachePolicy(); entries != 0 {
					b.Fatalf("expected disabled result caching, got entries=%d", entries)
				}
			} else {
				exec, _ = newTestExecutor(b)
			}
			ctx := context.Background()
			params := map[string]interface{}{"value": int64(7)}
			const query = "RETURN $value AS value"
			execute := exec.executeInternal
			if mode.public {
				warm, err := exec.Execute(ctx, query, params)
				if err != nil || len(warm.Rows) != 1 || len(warm.Rows[0]) != 1 || warm.Rows[0][0] != int64(7) {
					b.Fatalf("unexpected warm result: %v, %v", warm, err)
				}
				execute = exec.Execute
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				result, err := execute(ctx, query, params)
				if mode.public {
					if err != nil || len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != int64(7) {
						b.Fatalf("unexpected result: %v, %v", result, err)
					}
				} else if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}
