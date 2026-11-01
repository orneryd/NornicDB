package cypher

import (
	"context"
	"testing"
)

// A protocol transaction's fresh executor reuses the database executor's
// syntax verdicts (and the database executor sees the transaction's), so a
// statement is validated once, not once per explicit transaction.
func TestShareStatementCachesFromReusesSyntaxVerdicts(t *testing.T) {
	store := newTestMemoryEngine(t)
	base := NewStorageExecutor(store)
	const first = "MATCH (n:ShareCacheA) RETURN count(n) AS c"
	if _, err := base.Execute(context.Background(), first, nil); err != nil {
		t.Fatal(err)
	}

	tx := NewStorageExecutor(store)
	tx.ShareStatementCachesFrom(base)
	validator := syntaxStatementAccepted
	if !tx.hasCachedValidSyntax(first, validator) {
		t.Fatalf("transaction executor does not see the database executor's verdict")
	}
	const second = "MATCH (n:ShareCacheB) RETURN n.name AS name"
	if _, err := tx.Execute(context.Background(), second, nil); err != nil {
		t.Fatal(err)
	}
	if !base.hasCachedValidSyntax(second, validator) {
		t.Fatalf("database executor does not see the transaction executor's verdict")
	}
	if tx.ensureUpperQueryCache() != base.ensureUpperQueryCache() {
		t.Fatalf("upper-cased text cache is not shared")
	}

	// Sharing with itself or with nothing keeps the executor's own caches.
	own := tx.ensureSyntaxValidationCache()
	tx.ShareStatementCachesFrom(nil)
	tx.ShareStatementCachesFrom(tx)
	if tx.ensureSyntaxValidationCache() != own {
		t.Fatalf("ShareStatementCachesFrom(nil/self) replaced the cache")
	}
}
