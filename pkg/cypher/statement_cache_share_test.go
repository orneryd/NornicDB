package cypher

import (
	"context"
	"testing"
)

// Executors given the same StatementCaches share syntax verdicts both ways,
// so a statement is validated once per database, not once per executor.
func TestUseStatementCachesSharesSyntaxVerdicts(t *testing.T) {
	store := newTestMemoryEngine(t)
	base := NewStorageExecutor(store)
	const first = "MATCH (n:ShareCacheA) RETURN count(n) AS c"
	if _, err := base.Execute(context.Background(), first, nil); err != nil {
		t.Fatal(err)
	}

	tx := NewStorageExecutor(store)
	tx.UseStatementCaches(base.StatementCaches())
	if tx.StatementCaches() != base.StatementCaches() {
		t.Fatalf("the transaction executor does not use the database executor's caches")
	}
	validator := activeSyntaxValidator()
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

	// Caches made up front are used as given; nil keeps the executor's own.
	caches := NewStatementCaches()
	fresh := NewStorageExecutor(store)
	fresh.UseStatementCaches(caches)
	if fresh.StatementCaches() != caches {
		t.Fatalf("UseStatementCaches did not install the given caches")
	}
	own := tx.StatementCaches()
	tx.UseStatementCaches(nil)
	if tx.StatementCaches() != own {
		t.Fatalf("UseStatementCaches(nil) replaced the caches")
	}
}
