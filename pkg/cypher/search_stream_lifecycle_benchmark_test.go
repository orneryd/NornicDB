package cypher

import "testing"

func BenchmarkSearchStreamCachePolicy(b *testing.B) {
	for _, query := range []string{"RETURN 1", "MATCH (n) RETURN n LIMIT 10", "CALL db.retrieve($request)", "CALL db.rretrieve($request)"} {
		b.Run(query, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = isCacheableReadQuery(query)
			}
		})
	}
}
