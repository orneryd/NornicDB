package cypher

import (
	"context"
	"strconv"
	"testing"
	"time"
)

// These public-API fixtures also compile unchanged on the unmodified bases.
func BenchmarkStatementSchemaCache(b *testing.B) {
	query := "MATCH ()-[:A|`B|C`]->() RETURN count(*) AS n"
	result := &ExecuteResult{Columns: []string{"n"}, Rows: [][]interface{}{{int64(1)}}}
	b.Run("publication_hot", func(b *testing.B) {
		cache := NewSmartQueryCache(16)
		cache.Put(query, nil, result, time.Minute)
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			cache.Put(query, nil, result, time.Minute)
		}
	})
	b.Run("analysis_hot", func(b *testing.B) {
		analyzer := NewQueryAnalyzer(16)
		analyzer.Analyze(context.Background(), query)
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			analyzer.Analyze(context.Background(), query)
		}
	})
	b.Run("publication_cold", func(b *testing.B) {
		cache := NewSmartQueryCache(16)
		prefix := query + "_" + strconv.Itoa(b.N) + "_"
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			cache.Put(prefix+strconv.Itoa(index), nil, result, time.Minute)
		}
	})
}
