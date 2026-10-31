package cypher

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// statementCacheColdFixture is the canonical owner for the cold-family
// schedule used by both the sequential and concurrent runs.
type statementCacheColdFixture struct {
	base    *StorageExecutor
	members []*StorageExecutor
}

func newStatementCacheColdFixture(t *testing.T) *statementCacheColdFixture {
	t.Helper()
	store := newTestMemoryEngine(t)
	base := NewStorageExecutor(store)
	// Build all children before any member has populated either cache, across
	// the clone shapes a cold parent can hand ownership to: a plain clone, a
	// USE-style selected clone, and selected-first / plain-first chains.
	return &statementCacheColdFixture{
		base: base,
		members: []*StorageExecutor{
			base.cloneWithStorage(store),
			base.cloneWithStorage(store, selectedDatabaseStorage),
			base.cloneWithStorage(store, selectedDatabaseStorage).cloneWithStorage(store),
			base.cloneWithStorage(store).cloneWithStorage(store, selectedDatabaseStorage),
		},
	}
}

// all is the membership order every assertion reports on: the cold members,
// then the base they were cloned from.
func (f *statementCacheColdFixture) all() []*StorageExecutor {
	return append(append([]*StorageExecutor{}, f.members...), f.base)
}

// assertSharesBacksFirstChild proves that every member resolves the syntax
// verdict and the upper-text map that the first child established.
func (f *statementCacheColdFixture) assertSharesBacksFirstChild(t *testing.T, query string) {
	t.Helper()
	for i, executor := range f.all() {
		if !executor.hasCachedValidSyntax(query, syntaxStatementAccepted) {
			t.Errorf("member %d lost the first child's syntax verdict (query %q)", i, query)
		}
		if executor.ensureUpperQueryCache() != f.members[0].ensureUpperQueryCache() {
			t.Errorf("member %d lost the first child's upper-text cache (query %q)", i, query)
		}
	}
}

// assertConcurrentRequests runs per-request clones with distinct parameters
// and requires every request to return its own row and to keep the base's
// stable syntax and upper-text cache owners.
func (f *statementCacheColdFixture) assertConcurrentRequests(t *testing.T, requests int) {
	t.Helper()
	var wg sync.WaitGroup
	failures := make(chan error, requests)
	for i := range requests {
		wg.Go(func() {
			executor := f.base.cloneWithStorage(f.base.storage, selectedDatabaseStorage).cloneWithStorage(f.base.storage)
			result, err := executor.Execute(context.Background(), "RETURN $value AS value", map[string]interface{}{"value": int64(i)})
			if err != nil {
				failures <- err
				return
			}
			if len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != int64(i) {
				failures <- fmt.Errorf("request %d got rows %v", i, result.Rows)
			}
			if executor.ensureSyntaxValidationCache() != f.base.ensureSyntaxValidationCache() ||
				executor.ensureUpperQueryCache() != f.base.ensureUpperQueryCache() {
				failures <- fmt.Errorf("request %d lost its stable text-cache owner", i)
			}
		})
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
}

// statementCacheColdSchedule is one execution shape of the cold family.
type statementCacheColdSchedule struct {
	name       string
	concurrent bool
	requests   int
}

func statementCacheColdSchedules() []statementCacheColdSchedule {
	return []statementCacheColdSchedule{
		// Sequential: one child compiles, then every member must resolve to
		// that child's caches, with no goroutine schedule involved.
		{name: "sequential", requests: 1},
		// Concurrent: many clones execute with distinct parameters at once;
		// each must return its own row and keep the stable text-cache owner.
		{name: "concurrent", concurrent: true, requests: 12},
	}
}

const statementCacheColdQuery = "MATCH (n:ColdCacheFamily) RETURN count(n) AS c"

// TestStatementCacheFamilyColdClones owns warm-through-clone behavior for
// cold family members on both schedules that previously had separate
// entrypoints.
func TestStatementCacheFamilyColdClones(t *testing.T) {
	for _, schedule := range statementCacheColdSchedules() {
		t.Run(schedule.name, func(t *testing.T) {
			fixture := newStatementCacheColdFixture(t)
			if schedule.concurrent {
				fixture.assertConcurrentRequests(t, schedule.requests)
				return
			}
			if _, err := fixture.members[0].Execute(context.Background(), statementCacheColdQuery, nil); err != nil {
				t.Fatal(err)
			}
			fixture.assertSharesBacksFirstChild(t, statementCacheColdQuery)
		})
	}
}
