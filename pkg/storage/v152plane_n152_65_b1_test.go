package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// NOR-210 reproduction-only audit at d6ec7766 (backend v1.5.1).
//
// The reported behaviour is a relationship-type cache eviction: after two
// databases' relationship types are warm, dropping one database clears the
// relationship-type cache for both (two warm entries -> zero), including the
// entry belonging to the unrelated database whose facts are kept. These tests
// exercise the actual BadgerEngine DeleteByPrefix owner (the DROP DATABASE data
// path) and record the current behaviour. The unrelated facts must stay valid;
// the cache eviction is measured, not assumed. No production behaviour is
// changed here.

// seedRelationshipType writes one namespace's two endpoint nodes and `edges`
// parallel relationships of one type through the namespaced view a database
// consumer uses. IDs are the user IDs of that database.
func seedRelationshipType(tb testing.TB, engine *BadgerEngine, namespace, edgeType string, edges int) {
	tb.Helper()
	seedRelationshipTypeNamed(tb, engine, namespace, edgeType, "edge", edges)
}

func seedRelationshipTypeNamed(tb testing.TB, engine *BadgerEngine, namespace, edgeType, edgeName string, edges int) {
	tb.Helper()
	view := NewNamespacedEngine(engine, namespace)
	ctx := context.Background()
	for _, id := range []NodeID{"start", "end"} {
		_, err := view.CreateNode(ctx, &Node{ID: id, Labels: []string{"Endpoint"}})
		require.NoError(tb, err)
	}
	for i := 0; i < edges; i++ {
		require.NoError(tb, view.CreateEdge(ctx, &Edge{
			ID:        EdgeID(fmt.Sprintf("%s-%d", edgeName, i)),
			StartNode: "start",
			EndNode:   "end",
			Type:      edgeType,
		}))
	}
}

// warmRelationshipType reads one type through the namespaced view, which is the
// supported public route into the relationship-type cache.
func warmRelationshipType(tb testing.TB, view *NamespacedEngine, edgeType string, want int) []*Edge {
	tb.Helper()
	edges, err := view.GetEdgesByType(context.Background(), edgeType)
	require.NoError(tb, err)
	require.Len(tb, edges, want)
	return edges
}

// relationshipTypeCacheEntryCount and cachedRelationshipTypeIDs are direct
// assertions on the existing internal cache owner. Public warmth is not
// observable, so the audit reads the owner that decides cache hits. The cache
// stores engine-qualified IDs; namespaced reads project them to user IDs.
func relationshipTypeCacheEntryCount(engine *BadgerEngine) int {
	engine.edgeTypeCacheMu.RLock()
	defer engine.edgeTypeCacheMu.RUnlock()
	return len(engine.edgeTypeCache)
}

func cachedRelationshipTypeIDs(engine *BadgerEngine, edgeType string) []EdgeID {
	engine.edgeTypeCacheMu.RLock()
	defer engine.edgeTypeCacheMu.RUnlock()
	cached := engine.edgeTypeCache[edgeType]
	ids := make([]EdgeID, 0, len(cached))
	for _, edge := range cached {
		if edge != nil {
			ids = append(ids, edge.ID)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func auditSortedEdgeIDs(edges []*Edge) []EdgeID {
	ids := make([]EdgeID, 0, len(edges))
	for _, edge := range edges {
		ids = append(ids, edge.ID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// TestDeleteByPrefixWholeNamespaceEvictsUnrelatedWarmRelationshipTypeEntry is
// the central reproduction. Two databases have distinct relationship types and
// both are warm (two cache entries). Dropping one database leaves the unrelated
// database's facts valid, but the current DROP clears the whole
// relationship-type cache, so both entries are evicted (2 -> 0) and the next
// unrelated read must refill from storage.
func TestDeleteByPrefixWholeNamespaceEvictsUnrelatedWarmRelationshipTypeEntry(t *testing.T) {
	const (
		victimType = "VICTIM_REL"
		peerType   = "PEER_REL"
	)
	engine := newTestEngine(t)
	ctx := context.Background()

	seedRelationshipType(t, engine, "victim", victimType, 2)
	seedRelationshipType(t, engine, "peer", peerType, 2)
	victimView := NewNamespacedEngine(engine, "victim")
	peerView := NewNamespacedEngine(engine, "peer")

	require.Len(t, warmRelationshipType(t, victimView, victimType, 2), 2)
	require.Len(t, warmRelationshipType(t, peerView, peerType, 2), 2)
	require.Equal(t, 2, relationshipTypeCacheEntryCount(engine), "two databases' relationship types are warm")
	require.Equal(t, []EdgeID{"victim:edge-0", "victim:edge-1"}, cachedRelationshipTypeIDs(engine, victimType))
	require.Equal(t, []EdgeID{"peer:edge-0", "peer:edge-1"}, cachedRelationshipTypeIDs(engine, peerType))

	nodesDeleted, edgesDeleted, err := engine.DeleteByPrefix("victim:")
	require.NoError(t, err)
	require.EqualValues(t, 2, nodesDeleted)
	require.EqualValues(t, 2, edgesDeleted)

	// Reproduction of the reported behaviour: the whole database DROP evicts the
	// unrelated database's warm relationship-type entry too (two entries -> zero).
	// It is not a global-clear baseline: the entries were warmed through public
	// reads and the DROP itself is the only invalidator.
	require.Zero(t, relationshipTypeCacheEntryCount(engine), "whole-database DROP clears the relationship-type cache")
	require.Empty(t, cachedRelationshipTypeIDs(engine, victimType))
	require.Empty(t, cachedRelationshipTypeIDs(engine, peerType), "the unrelated relationship type entry is evicted with the dropped one")

	// Unrelated KEEP: the peer facts must remain valid after the eviction. The
	// next read is a cold refill from the durable edge-type index.
	kept, err := peerView.GetEdgesByType(ctx, peerType)
	require.NoError(t, err)
	require.Equal(t, []EdgeID{"edge-0", "edge-1"}, auditSortedEdgeIDs(kept), "unrelated relationship facts are retained")
	require.Equal(t, []EdgeID{"peer:edge-0", "peer:edge-1"}, cachedRelationshipTypeIDs(engine, peerType), "the cold read refills the unrelated entry")
	require.Empty(t, cachedRelationshipTypeIDs(engine, victimType), "the dropped type is never refilled")

	// Target namespace gone.
	dropped, err := victimView.GetEdgesByType(ctx, victimType)
	require.NoError(t, err)
	require.Empty(t, dropped)

	// Counters follow the durable facts, not the cache.
	count, err := engine.EdgeCountByTypeInNamespace(ctx, "peer", peerType)
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
	count, err = engine.EdgeCountByTypeInNamespace(ctx, "victim", victimType)
	require.NoError(t, err)
	require.Zero(t, count)
	count, err = engine.EdgeCountByType(ctx, peerType)
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
	count, err = engine.EdgeCountByType(ctx, victimType)
	require.NoError(t, err)
	require.Zero(t, count)

	_, err = peerView.GetNode(ctx, "start")
	require.NoError(t, err)
	_, err = victimView.GetNode(ctx, "start")
	require.ErrorIs(t, err, ErrNotFound)
}

// TestDeleteByPrefixPartialPrefixRetainsKeptRelationshipFacts covers the partial
// prefix owner inside one namespace: deleted rows must leave the relationship
// type result, kept rows and the namespace counter must remain exact.
func TestDeleteByPrefixPartialPrefixRetainsKeptRelationshipFacts(t *testing.T) {
	const relType = "KNOWS"
	engine := newTestEngine(t)
	ctx := context.Background()

	for _, group := range []string{"drop", "keep"} {
		for _, endpoint := range []string{"start", "end"} {
			_, err := engine.CreateNode(ctx, &Node{ID: NodeID("audit:" + group + "-" + endpoint), Labels: []string{"Endpoint"}})
			require.NoError(t, err)
		}
		for i := 0; i < 2; i++ {
			require.NoError(t, engine.CreateEdge(ctx, &Edge{
				ID:        EdgeID(fmt.Sprintf("audit:%s-%d", group, i)),
				StartNode: NodeID("audit:" + group + "-start"),
				EndNode:   NodeID("audit:" + group + "-end"),
				Type:      relType,
			}))
		}
	}

	all, err := engine.GetEdgesByType(ctx, relType)
	require.NoError(t, err)
	require.Len(t, all, 4)
	require.Len(t, cachedRelationshipTypeIDs(engine, relType), 4, "both groups share one warm relationship type")

	nodesDeleted, edgesDeleted, err := engine.DeleteByPrefix("audit:drop-")
	require.NoError(t, err)
	require.EqualValues(t, 2, nodesDeleted)
	require.EqualValues(t, 2, edgesDeleted)

	kept, err := engine.GetEdgesByType(ctx, relType)
	require.NoError(t, err)
	require.Equal(t, []EdgeID{"audit:keep-0", "audit:keep-1"}, auditSortedEdgeIDs(kept), "dropped rows must leave the cached-or-refilled type result")
	require.NotContains(t, cachedRelationshipTypeIDs(engine, relType), EdgeID("audit:drop-0"))
	require.NotContains(t, cachedRelationshipTypeIDs(engine, relType), EdgeID("audit:drop-1"))

	count, err := engine.EdgeCountByTypeInNamespace(ctx, "audit", relType)
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
	_, err = engine.GetEdge(ctx, "audit:drop-0")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = engine.GetEdge(ctx, "audit:keep-0")
	require.NoError(t, err)
}

// TestDeleteByPrefixDropAndRecreateReplacesRelationshipTypeFacts covers the
// supported drop/recreate sequence: the recreated database's fresh relationships
// are served, the previous generation's relationships never reappear, and the
// unrelated database keeps its facts.
func TestDeleteByPrefixDropAndRecreateReplacesRelationshipTypeFacts(t *testing.T) {
	const (
		victimType = "VICTIM_REL"
		peerType   = "PEER_REL"
	)
	engine := newTestEngine(t)
	ctx := context.Background()

	seedRelationshipType(t, engine, "victim", victimType, 2)
	seedRelationshipType(t, engine, "peer", peerType, 2)
	victimView := NewNamespacedEngine(engine, "victim")
	peerView := NewNamespacedEngine(engine, "peer")
	warmRelationshipType(t, victimView, victimType, 2)
	warmRelationshipType(t, peerView, peerType, 2)
	require.Equal(t, 2, relationshipTypeCacheEntryCount(engine))

	_, _, err := engine.DeleteByPrefix("victim:")
	require.NoError(t, err)
	seedRelationshipTypeNamed(t, engine, "victim", victimType, "fresh", 2)

	recreated, err := victimView.GetEdgesByType(ctx, victimType)
	require.NoError(t, err)
	require.Equal(t, []EdgeID{"fresh-0", "fresh-1"}, auditSortedEdgeIDs(recreated))
	require.NotContains(t, cachedRelationshipTypeIDs(engine, victimType), EdgeID("victim:edge-0"))

	kept, err := peerView.GetEdgesByType(ctx, peerType)
	require.NoError(t, err)
	require.Equal(t, []EdgeID{"edge-0", "edge-1"}, auditSortedEdgeIDs(kept))
	count, err := engine.EdgeCountByTypeInNamespace(ctx, "victim", victimType)
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
}

// TestDeleteByPrefixInterruptedDropKeepsUnrelatedRelationshipTypeRead uses the
// existing dropPrefixTailHook fault fixture: the DROP fails after row removal but
// keeps its durable fence. Unrelated scalar/counter reads stay exact, the victim
// namespace and the engine-global relationship-type read stay fenced (fail
// closed, never stale or zero), and the DROP owner finishes the retry.
func TestDeleteByPrefixInterruptedDropKeepsUnrelatedRelationshipTypeRead(t *testing.T) {
	const (
		victimType = "VICTIM_REL"
		peerType   = "PEER_REL"
	)
	engine := newTestEngine(t)
	ctx := context.Background()

	seedRelationshipType(t, engine, "victim", victimType, 2)
	seedRelationshipType(t, engine, "peer", peerType, 2)
	victimView := NewNamespacedEngine(engine, "victim")
	peerView := NewNamespacedEngine(engine, "peer")
	warmRelationshipType(t, victimView, victimType, 2)
	warmRelationshipType(t, peerView, peerType, 2)
	require.Equal(t, 2, relationshipTypeCacheEntryCount(engine))

	sentinel := errors.New("intrinsic DROP cleanup unavailable")
	engine.dropPrefixTailHook = func(prefix string) error {
		if prefix == "victim:" {
			return sentinel
		}
		return nil
	}
	t.Cleanup(func() { engine.dropPrefixTailHook = nil })

	_, _, err := engine.DeleteByPrefix("victim:")
	require.ErrorIs(t, err, sentinel)
	require.ErrorIs(t, engine.pendingNamespaceConflict("victim"), ErrConflict, "the durable DROP fence is held")

	// Unrelated facts stay exact while the victim cleanup is unfinished: the
	// namespace-scoped point and counter reads answer, and the engine-global
	// relationship-type read fails closed instead of serving the evicted cache or
	// a partial/zero population.
	_, err = peerView.GetNode(ctx, "start")
	require.NoError(t, err)
	count, err := engine.EdgeCountByTypeInNamespace(ctx, "peer", peerType)
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
	_, err = peerView.GetEdgesByType(ctx, peerType)
	require.ErrorIs(t, err, ErrConflict, "an unrelated engine-global type read is bounded by the pending DROP fence")

	// Canonical fence controls: warm point and namespace-count reads of the
	// pending namespace are rejected.
	_, err = engine.GetNode(ctx, "victim:start")
	require.ErrorIs(t, err, ErrConflict)
	_, err = engine.EdgeCountByTypeInNamespace(ctx, "victim", victimType)
	require.ErrorIs(t, err, ErrConflict)

	// The same DROP owner finishes the interrupted work.
	engine.dropPrefixTailHook = nil
	nodesDeleted, edgesDeleted, err := engine.DeleteByPrefix("victim:")
	require.NoError(t, err)
	require.EqualValues(t, 2, nodesDeleted)
	require.EqualValues(t, 2, edgesDeleted)
	require.NoError(t, engine.pendingNamespaceConflict("victim"))

	kept, err := peerView.GetEdgesByType(ctx, peerType)
	require.NoError(t, err)
	require.Equal(t, []EdgeID{"edge-0", "edge-1"}, auditSortedEdgeIDs(kept))
	dropped, err := victimView.GetEdgesByType(ctx, victimType)
	require.NoError(t, err)
	require.Empty(t, dropped)
	count, err = engine.EdgeCountByType(ctx, peerType)
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
	count, err = engine.EdgeCountByType(ctx, victimType)
	require.NoError(t, err)
	require.Zero(t, count)
}

// TestDeleteByPrefixBlockedDropKeepsUnrelatedRelationshipTypeRead holds the DROP
// at its existing post-row-removal hook and reads the unrelated database while
// the destructive cut is in progress. The unrelated scoped reads must stay exact
// on every bounded attempt and the engine-global type read must fail closed; no
// stress loop or timing threshold is involved.
func TestDeleteByPrefixBlockedDropKeepsUnrelatedRelationshipTypeRead(t *testing.T) {
	const (
		victimType = "VICTIM_REL"
		peerType   = "PEER_REL"
	)
	engine := newTestEngine(t)
	ctx := context.Background()

	seedRelationshipType(t, engine, "victim", victimType, 2)
	seedRelationshipType(t, engine, "peer", peerType, 2)
	victimView := NewNamespacedEngine(engine, "victim")
	peerView := NewNamespacedEngine(engine, "peer")
	warmRelationshipType(t, victimView, victimType, 2)
	warmRelationshipType(t, peerView, peerType, 2)
	require.Equal(t, 2, relationshipTypeCacheEntryCount(engine))

	entered, release := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	dropFinished := make(chan struct{})
	engine.dropPrefixTailHook = func(prefix string) error {
		if prefix == "victim:" {
			enterOnce.Do(func() { close(entered) })
			<-release
		}
		return nil
	}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		<-dropFinished
		engine.dropPrefixTailHook = nil
	})

	dropDone := make(chan error, 1)
	go func() {
		defer close(dropFinished)
		_, _, err := engine.DeleteByPrefix("victim:")
		dropDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("DROP did not reach its existing row-removal tail hook")
	}

	for attempt := 0; attempt < 4; attempt++ {
		_, err := peerView.GetNode(ctx, "start")
		require.NoError(t, err, "attempt %d", attempt)
		count, err := engine.EdgeCountByTypeInNamespace(ctx, "peer", peerType)
		require.NoError(t, err, "attempt %d", attempt)
		require.EqualValues(t, 2, count, "attempt %d", attempt)
		_, err = peerView.GetEdgesByType(ctx, peerType)
		require.ErrorIs(t, err, ErrConflict, "attempt %d: engine-global type read fails closed while the DROP owns the cut", attempt)
	}

	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-dropDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("DROP did not finish after the tail hook released")
	}

	kept, err := peerView.GetEdgesByType(ctx, peerType)
	require.NoError(t, err)
	require.Equal(t, []EdgeID{"edge-0", "edge-1"}, auditSortedEdgeIDs(kept))
	dropped, err := victimView.GetEdgesByType(ctx, victimType)
	require.NoError(t, err)
	require.Empty(t, dropped)
}
