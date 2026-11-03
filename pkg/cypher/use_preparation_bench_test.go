package cypher

import (
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Keep the public API and result assertion identical in baseline/current builds.
func BenchmarkUsePreparationPublic(b *testing.B) {
	for _, spaces := range []int{1, 1024, 32 << 10} {
		b.Run(fmt.Sprintf("spaces_%d", spaces), func(b *testing.B) {
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "use_preparation_bench"))
			e.cache = nil
			e.planCache = nil
			query := "USE" + strings.Repeat(" ", spaces) + "`use_preparation_bench` RETURN 7 AS k ORDER BY k"
			ctx := b.Context()
			result, err := e.Execute(ctx, query, nil)
			if err != nil || len(result.Rows) != 1 || result.Rows[0][0] != int64(7) {
				b.Fatalf("incorrect USE preparation result: %+v, %v", result, err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := e.Execute(ctx, query, nil)
				if err != nil || len(result.Rows) != 1 || result.Rows[0][0] != int64(7) {
					b.Fatalf("incorrect USE preparation result: %+v, %v", result, err)
				}
			}
		})
	}
}
