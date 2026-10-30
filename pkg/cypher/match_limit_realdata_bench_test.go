//go:build integration
// +build integration

package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"
)

const realDataMatchLimitQueryTemplate = `
MATCH (n)
RETURN n
LIMIT %d /* cache_bust_%d */`

func BenchmarkRealData_MatchLimit_CacheMiss(b *testing.B) {
	exec, cleanup := openRealDataBenchmarkExecutor(b, "caremark_translation")
	defer cleanup()

	exec.cache = nil

	limits := []int{5, 10, 25}
	for _, limit := range limits {
		limit := limit
		b.Run(fmt.Sprintf("limit_%d", limit), func(b *testing.B) {
			{
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				_, _ = exec.Execute(ctx, fmt.Sprintf(realDataMatchLimitQueryTemplate, limit, -1), nil)
				cancel()
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				q := fmt.Sprintf(realDataMatchLimitQueryTemplate, limit, i)
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				res, err := exec.Execute(ctx, q, nil)
				cancel()
				if err != nil {
					b.Fatalf("execute failed at iter %d: %v", i, err)
				}
				if res == nil || len(res.Columns) == 0 {
					b.Fatalf("unexpected empty result metadata at iter %d", i)
				}
				if len(res.Rows) > limit {
					b.Fatalf("result row count %d exceeded LIMIT %d at iter %d", len(res.Rows), limit, i)
				}
			}
		})
	}
}
