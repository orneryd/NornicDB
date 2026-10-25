package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Measure property-index numeric seeks through the public executor: an
// equality seek, a two-sided range and an index-ordered page, over ordinary
// integers and integers beyond 2^53.
func BenchmarkNumericPropertyIndexSeek(b *testing.B) {
	for _, data := range []struct {
		name string
		base int64
	}{{"small", 0}, {"beyond_2p53", 1 << 53}} {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "test")
		exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
		ctx := context.Background()
		if _, err := exec.Execute(ctx, "CREATE RANGE INDEX bench_numeric FOR (n:BenchNumeric) ON (n.v)", nil); err != nil {
			b.Fatal(err)
		}
		values := make([]interface{}, 2000)
		for i := range values {
			values[i] = data.base + int64(i)
		}
		if _, err := exec.Execute(ctx, "UNWIND $vs AS v CREATE (:BenchNumeric {v:v})", map[string]interface{}{"vs": values}); err != nil {
			b.Fatal(err)
		}
		for _, query := range []struct{ name, text string }{
			{"equality", "MATCH (n:BenchNumeric) WHERE n.v = $lo RETURN n.v"},
			{"range", "MATCH (n:BenchNumeric) WHERE n.v > $lo AND n.v <= $hi RETURN count(n)"},
			{"ordered_page", "MATCH (n:BenchNumeric) WHERE n.v >= $lo RETURN n.v ORDER BY n.v LIMIT 10"},
		} {
			params := map[string]interface{}{"lo": data.base + 1001, "hi": data.base + 1101}
			b.Run(data.name+"/"+query.name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, err := exec.Execute(ctx, query.text, params); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
