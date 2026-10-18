package cypher

import (
	"context"
	"testing"
)

// BenchmarkProtocolTransactionStatementCaches measures the fresh-executor path
// used for each Bolt/HTTP transaction, after the database has seen the statement.
// The interface keeps the same benchmark runnable on the release baseline.
func BenchmarkProtocolTransactionStatementCaches(b *testing.B) {
	store := newTestMemoryEngine(b)
	base := NewStorageExecutor(store)
	const query = "MATCH (n:ShareCacheBench) WHERE n.id = $id RETURN n.name AS name"
	ctx := context.Background()
	if _, err := base.Execute(ctx, query, map[string]interface{}{"id": -1}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := NewStorageExecutor(store)
		if sharing, ok := interface{}(tx).(interface{ ShareStatementCachesFrom(*StorageExecutor) }); ok {
			sharing.ShareStatementCachesFrom(base)
		}
		if _, err := tx.Execute(ctx, query, map[string]interface{}{"id": i}); err != nil {
			b.Fatal(err)
		}
	}
}
