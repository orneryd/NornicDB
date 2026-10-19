package cypher

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestStatementCacheFamilyColdClones(t *testing.T) {
	store := newTestMemoryEngine(t)
	base := NewStorageExecutor(store)
	// Build all children before any member has populated either cache.
	clones := []*StorageExecutor{
		base.cloneWithStorage(store),
		base.cloneForStorage(store),
		base.cloneForStorage(store).cloneWithStorage(store),
		base.cloneWithStorage(store).cloneForStorage(store),
	}
	const query = "MATCH (n:ColdCacheFamily) RETURN count(n) AS c"
	if _, err := clones[0].Execute(context.Background(), query, nil); err != nil {
		t.Fatal(err)
	}
	for i, executor := range append(clones, base) {
		if !executor.hasCachedValidSyntax(query, activeSyntaxValidator()) {
			t.Errorf("member %d lost the first child's syntax verdict", i)
		}
		if executor.ensureUpperQueryCache() != clones[0].ensureUpperQueryCache() {
			t.Errorf("member %d lost the first child's upper-text cache", i)
		}
	}
}

func TestStatementCacheFamilyConcurrentColdClones(t *testing.T) {
	store := newTestMemoryEngine(t)
	base := NewStorageExecutor(store)
	var wg sync.WaitGroup
	errors := make(chan error, 12)
	for i := range 12 {
		wg.Go(func() {
			executor := base.cloneForStorage(store).cloneWithStorage(store)
			result, err := executor.Execute(context.Background(), "RETURN $value AS value", map[string]interface{}{"value": int64(i)})
			if err != nil {
				errors <- err
				return
			}
			if len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != int64(i) {
				errors <- fmt.Errorf("request %d got rows %v", i, result.Rows)
			}
			if executor.ensureSyntaxValidationCache() != base.ensureSyntaxValidationCache() || executor.ensureUpperQueryCache() != base.ensureUpperQueryCache() {
				errors <- fmt.Errorf("request %d lost its stable text-cache owner", i)
			}
		})
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}
