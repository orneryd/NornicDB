package cypher

import (
	"context"
	"sync"
	"testing"
)

func TestStatementCacheFamilyDispose(t *testing.T) {
	store := newTestMemoryEngine(t)
	base := NewStorageExecutor(store)
	child := base.cloneForStorage(store).cloneWithStorage(store)
	owner := base.StatementCaches()
	const query = "RETURN 42 AS value"
	if _, err := child.Execute(context.Background(), query, nil); err != nil {
		t.Fatal(err)
	}
	if !base.hasCachedValidSyntax(query, activeSyntaxValidator()) {
		t.Fatal("cold family did not share verdict")
	}
	owner.Dispose()
	owner.Dispose() // Owners may be retired concurrently by shutdown and metadata removal.
	for _, executor := range []*StorageExecutor{base, child, base.cloneForStorage(store)} {
		if _, err := executor.Execute(context.Background(), query, nil); err != nil {
			t.Fatal(err)
		}
		assertStatementCachesEmpty(t, executor.StatementCaches())
	}
}

func TestStatementCacheFamilyConcurrentDispose(t *testing.T) {
	store := newTestMemoryEngine(t)
	base := NewStorageExecutor(store)
	owner := base.StatementCaches()
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			child := base.cloneForStorage(store)
			for range 100 {
				child.markCachedValidSyntax("RETURN 42 AS value", activeSyntaxValidator())
				child.cachedUpperQuery("return 42 as value")
				child.hasCachedValidSyntax("RETURN 42 AS value", activeSyntaxValidator())
			}
		})
	}
	wg.Go(owner.Dispose)
	wg.Go(owner.Dispose)
	wg.Wait()
	assertStatementCachesEmpty(t, owner)
}

func assertStatementCachesEmpty(t *testing.T, owner *StatementCaches) {
	t.Helper()
	if !owner.Disposed() {
		t.Fatal("owner was not retired")
	}
	owner.upper.mu.RLock()
	entries := len(owner.upper.cache)
	owner.upper.mu.RUnlock()
	for i := range owner.syntax.shards {
		sh := &owner.syntax.shards[i]
		sh.mu.RLock()
		entries += len(sh.cache)
		sh.mu.RUnlock()
	}
	if entries != 0 {
		t.Fatalf("retired owner retained %d entries", entries)
	}
}
