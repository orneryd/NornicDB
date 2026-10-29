package cypher

import (
	"context"
	"sync"
	"testing"
)

// statementCacheDisposeFixture is the canonical owner for the retire-while-
// in-use families. Every member is a clone of one base executor, so each
// member shares the base's compiled verdicts and its canonical
// StatementCaches owner. Disposal is a property of that owner, never of an
// individual clone: a retired owner must stay empty and must stay usable
// through every surviving member, whether it was retired sequentially or
// while members were still compiling under it.
type statementCacheDisposeFixture struct {
	base  *StorageExecutor
	owner *StatementCaches
	// query is the statement every member recompiles after retirement.
	query string
	// key is the statement text the concurrent schedule records directly in
	// the shared caches on every iteration.
	key string
	// members are clone chains that all resolve to the canonical owner: a
	// USE-style selected-database clone, a plain clone, and a clone of a
	// selected clone.
	members []*StorageExecutor
}

func newStatementCacheDisposeFixture(t *testing.T) *statementCacheDisposeFixture {
	t.Helper()
	store := newTestMemoryEngine(t)
	base := NewStorageExecutor(store)
	return &statementCacheDisposeFixture{
		base:  base,
		owner: base.StatementCaches(),
		query: "RETURN 42 AS value",
		key:   "RETURN 42 AS value",
		members: []*StorageExecutor{
			base.cloneWithStorage(store, selectedDatabaseStorage),
			base.cloneWithStorage(store),
			base.cloneWithStorage(store, selectedDatabaseStorage).cloneWithStorage(store),
			base,
		},
	}
}

// warm is the cold-family step: one child compiles the family's query, and
// the base must then resolve that verdict through the canonical owner.
func (f *statementCacheDisposeFixture) warm(t *testing.T) {
	t.Helper()
	if _, err := f.members[2].Execute(context.Background(), f.query, nil); err != nil {
		t.Fatal(err)
	}
	if !f.base.hasCachedValidSyntax(f.query, activeSyntaxValidator()) {
		t.Fatal("cold family did not share verdict")
	}
}

// churn is the concurrent schedule's per-member work: under the shared owner
// it keeps recording the same verdict and upper-text entry while retirement
// runs.
func (f *statementCacheDisposeFixture) churn(child *StorageExecutor, iterations int) {
	for range iterations {
		child.markCachedValidSyntax(f.key, activeSyntaxValidator())
		child.cachedUpperQuery("return 42 as value")
		child.hasCachedValidSyntax(f.key, activeSyntaxValidator())
	}
}

// assertRetiredForMembers proves the recompile path and the cache ownership
// after retirement: every member recompiles the query successfully and still
// resolves the retired, empty canonical owner.
func (f *statementCacheDisposeFixture) assertRetiredForMembers(t *testing.T) {
	t.Helper()
	for i, executor := range f.members {
		if _, err := executor.Execute(context.Background(), f.query, nil); err != nil {
			t.Fatalf("member %d failed to recompile after retirement: %v", i, err)
		}
		assertStatementCachesEmpty(t, executor.StatementCaches())
		if executor.StatementCaches() != f.owner {
			t.Fatalf("member %d resolved a cache owner other than the retired canonical owner", i)
		}
	}
}

// statementCacheSchedule is one execution shape of the dispose family.
type statementCacheSchedule struct {
	name       string
	disposals  int
	concurrent bool
	members    int
	iterations int
}

func statementCacheSchedules() []statementCacheSchedule {
	return []statementCacheSchedule{
		// Sequential: warm the family, retire twice back to back (shutdown,
		// then database-metadata removal), then recompile through members.
		{name: "sequential", disposals: 2},
		// Concurrent: retirement races members that keep recording verdicts
		// and upper-text entries; the owner must still end empty.
		{name: "concurrent", disposals: 2, concurrent: true, members: 12, iterations: 100},
	}
}

// TestStatementCacheFamilyDispose owns the retire-while-in-use contract for
// both schedules that previously lived in separate entrypoints.
func TestStatementCacheFamilyDispose(t *testing.T) {
	for _, schedule := range statementCacheSchedules() {
		t.Run(schedule.name, func(t *testing.T) {
			fixture := newStatementCacheDisposeFixture(t)
			fixture.warm(t)
			var wg sync.WaitGroup
			if schedule.concurrent {
				for i := range schedule.members {
					member := fixture.members[i%len(fixture.members)]
					wg.Go(func() { fixture.churn(member, schedule.iterations) })
				}
			}
			for range schedule.disposals {
				if !schedule.concurrent {
					fixture.owner.Dispose()
					continue
				}
				wg.Go(fixture.owner.Dispose)
			}
			wg.Wait()
			fixture.assertRetiredForMembers(t)
		})
	}
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
