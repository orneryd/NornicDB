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

// The path helper retains copies of the values a caller supplies to it. Those
// copies are ordinary value-copy callbacks and must run outside the cache
// mutex.
func TestWarmupPathCopiesRunOutsideCacheMutex(t *testing.T) {
	fixture := newOwnershipWarmupFixture(t)
	var armed, blocked atomic.Bool
	var copies atomic.Int64
	probe := cacheCopyLockProbe{fixture.functions.cache, &armed, &blocked, &copies}
	armed.Store(true)

	result := fixture.functions.Path(
		[]*Node{{ID: 7, Labels: []string{"Probed"}, Properties: map[string]interface{}{"probe": probe}}},
		[]*Relationship{{ID: 8, StartNode: 7, EndNode: 7, Type: "PROBED", Properties: map[string]interface{}{"probe": probe}}},
	)
	require.Equal(t, 1, result["nodesLoaded"], "the helper still reports the node it was given")
	require.Equal(t, 1, result["relationshipsLoaded"], "the helper still reports the relationship it was given")
	require.Equal(t, int64(2), copies.Load(), "the supplied node and relationship each run their copy protocol")
	require.False(t, blocked.Load(), "path copy callbacks must be able to acquire the cache mutex")

	cachedNode, ok := fixture.functions.GetCachedNode(7)
	require.True(t, ok, "the path helper still retains the node it was given")
	require.Equal(t, []string{"Probed"}, cachedNode.Labels)
	require.Contains(t, cachedNode.Properties, "probe", "the retained node keeps the supplied property")
	cachedRel, ok := fixture.functions.GetCachedRelationship(8)
	require.True(t, ok, "the path helper still retains the relationship it was given")
	require.Equal(t, "PROBED", cachedRel.Type)

	empty := fixture.functions.Path(nil, nil)
	require.Zero(t, empty["nodesLoaded"], "an empty path still reports no nodes")
	require.Zero(t, empty["relationshipsLoaded"], "an empty path still reports no relationships")
}

// The subgraph helper retains a copy of the start node a caller supplies to it
// and retains the records its backend reads return, which the backend copies.
// Neither those copies nor the backend reads themselves may run under the cache
// mutex.
func TestWarmupSubgraphCopiesRunOutsideCacheMutex(t *testing.T) {
	fixture := newOwnershipWarmupFixture(t)
	var armed atomic.Bool
	var startBlocked, neighborBlocked, relationshipBlocked atomic.Bool
	var startCopies, neighborCopies, relationshipCopies atomic.Int64
	startProbe := cacheCopyLockProbe{fixture.functions.cache, &armed, &startBlocked, &startCopies}
	neighborProbe := cacheCopyLockProbe{fixture.functions.cache, &armed, &neighborBlocked, &neighborCopies}
	relationshipProbe := cacheCopyLockProbe{fixture.functions.cache, &armed, &relationshipBlocked, &relationshipCopies}

	// bob is a neighbor the start node's backend neighbor read returns; the
	// relationship is one the start node's backend relationship read returns.
	require.NoError(t, fixture.backend.UpdateNode(t.Context(), fixture.bob, storage.PropertyUpdate{Set: map[string]interface{}{"probe": neighborProbe}}))
	require.NoError(t, fixture.backend.UpdateRelationship(t.Context(), fixture.worksAtAlice, storage.PropertyUpdate{Set: map[string]interface{}{"probe": relationshipProbe}}))
	start, err := fixture.backend.GetNode(t.Context(), fixture.alice)
	require.NoError(t, err)
	start.Properties["probe"] = startProbe

	armed.Store(true)
	result, err := fixture.functions.Subgraph(t.Context(), start, 1)
	require.NoError(t, err)
	require.Equal(t, 3, result["nodesLoaded"], "the subgraph still reports the start node and its neighbors")
	require.Equal(t, 2, result["relationshipsLoaded"], "the subgraph still reports the relationships it read")
	require.Positive(t, startCopies.Load(), "the supplied start node runs its copy protocol")
	require.Positive(t, neighborCopies.Load(), "a backend neighbor read runs its copy protocol")
	require.Positive(t, relationshipCopies.Load(), "a backend relationship read runs its copy protocol")
	require.False(t, startBlocked.Load(), "the supplied start node copy must run outside the cache mutex")
	require.False(t, neighborBlocked.Load(), "backend neighbor reads and their copies must run outside the cache mutex")
	require.False(t, relationshipBlocked.Load(), "backend relationship reads and their copies must run outside the cache mutex")
}

func TestWarmupProgressCopiesRunOutsideCacheMutex(t *testing.T) {
	fixture := newOwnershipWarmupFixture(t)
	var armed, blocked atomic.Bool
	var copies atomic.Int64
	probe := cacheCopyLockProbe{fixture.functions.cache, &armed, &blocked, &copies}
	require.NoError(t, fixture.backend.UpdateNode(t.Context(), fixture.alice, storage.PropertyUpdate{Set: map[string]interface{}{"probe": probe}}))
	require.NoError(t, fixture.backend.UpdateRelationship(t.Context(), fixture.worksAtAlice, storage.PropertyUpdate{Set: map[string]interface{}{"probe": probe}}))
	fixture.warm(t)
	armed.Store(true)
	progress, err := fixture.functions.Progress(t.Context())
	require.NoError(t, err)
	require.Equal(t, float64(100), progress["percentage"])
	require.Equal(t, progress["totalNodes"], progress["nodesLoaded"])
	require.Equal(t, progress["totalRels"], progress["relsLoaded"])
	require.Positive(t, copies.Load())
	require.False(t, blocked.Load(), "progress backend copy callbacks must run outside the cache mutex")
}
