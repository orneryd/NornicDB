package warmup

import (
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/apoc/storage"
	"github.com/stretchr/testify/require"
)

type cacheCopyLockProbe struct {
	cache   *WarmupCache
	armed   *atomic.Bool
	blocked *atomic.Bool
	copies  *atomic.Int64
}

func (value cacheCopyLockProbe) CallerCopy() any {
	if value.armed.Load() {
		value.copies.Add(1)
		if value.cache.mu.TryLock() {
			value.cache.mu.Unlock()
		} else {
			value.blocked.Store(true)
		}
	}
	return value
}

// A supported value-copy callback may read cache statistics. Prove it runs
// outside the cache mutex without making a test goroutine actually deadlock.
func TestWarmupCopiesRunOutsideCacheMutex(t *testing.T) {
	fixture := newOwnershipWarmupFixture(t)
	var armed, blocked atomic.Bool
	var copies atomic.Int64
	probe := cacheCopyLockProbe{fixture.functions.cache, &armed, &blocked, &copies}
	require.NoError(t, fixture.backend.UpdateNode(t.Context(), fixture.alice, storage.PropertyUpdate{Set: map[string]interface{}{"probe": probe}}))
	require.NoError(t, fixture.backend.UpdateRelationship(t.Context(), fixture.worksAtAlice, storage.PropertyUpdate{Set: map[string]interface{}{"probe": probe}}))
	fixture.warm(t)
	armed.Store(true)
	for _, read := range []struct {
		name string
		run  func() bool
	}{
		{"node", func() bool { _, ok := fixture.functions.GetCachedNode(fixture.alice); return ok }},
		{"relationship", func() bool { _, ok := fixture.functions.GetCachedRelationship(fixture.worksAtAlice); return ok }},
		{"label", func() bool { _, ok := fixture.functions.GetNodesByLabel("Person"); return ok }},
		{"type", func() bool { _, ok := fixture.functions.GetRelationshipsByType("WORKS_AT"); return ok }},
	} {
		t.Run(read.name, func(t *testing.T) {
			copies.Store(0)
			blocked.Store(false)
			require.True(t, read.run())
			require.Positive(t, copies.Load(), "the delivered payload must really run its copy protocol")
			require.False(t, blocked.Load(), "copy callbacks must be able to acquire the cache mutex")
		})
	}
}
