package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// N152-65 / NOR-210: the relationship-type cache is engine-global, but a
// prefix DROP only removes the relationships of the prefixes it names. A DROP
// must therefore revoke exactly the types that lost relationships to it,
// instead of evicting every warm type result, while a type whose result spans a
// dropped prefix is revoked so no dropped relationship survives in a cached
// body. Public warmth is not observable, so these tests also read the existing
// cache owner that decides type-cache hits.
const (
	dropCacheVictimType = "NOR210_VICTIM"
	dropCachePeerType   = "NOR210_KEEP"
	dropCacheSharedType = "NOR210_SHARED"
)

// seedDropCacheRelationshipType stores count relationships of edgeType between
// two nodes of namespace.
func seedDropCacheRelationshipType(t *testing.T, engine *BadgerEngine, namespace, edgeType string, count int) {
	t.Helper()
	ctx := context.Background()
	for _, end := range []string{"start", "end"} {
		_, err := engine.CreateNode(ctx, &Node{ID: NodeID(namespace + ":" + end), Labels: []string{"Endpoint"}})
		require.NoError(t, err)
	}
	for i := 0; i < count; i++ {
		require.NoError(t, engine.CreateEdge(ctx, &Edge{
			ID:        EdgeID(fmt.Sprintf("%s:edge-%d", namespace, i)),
			StartNode: NodeID(namespace + ":start"),
			EndNode:   NodeID(namespace + ":end"),
			Type:      edgeType,
		}))
	}
}

// warmDropCacheRelationshipType reads edgeType once and returns the sorted
// relationship IDs it served, so the test can compare a public result with the
// warm cache body that answers the next read.
func warmDropCacheRelationshipType(t *testing.T, engine *BadgerEngine, edgeType string) []EdgeID {
	t.Helper()
	edges, err := engine.GetEdgesByType(context.Background(), edgeType)
	require.NoError(t, err)
	return dropCacheSortedEdgeIDs(edges)
}

func dropCacheSortedEdgeIDs(edges []*Edge) []EdgeID {
	ids := make([]EdgeID, 0, len(edges))
	for _, edge := range edges {
		ids = append(ids, edge.ID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// cachedDropCacheRelationshipTypeIDs returns the relationship IDs the type
// cache currently holds for edgeType.
func cachedDropCacheRelationshipTypeIDs(t *testing.T, engine *BadgerEngine, edgeType string) []EdgeID {
	t.Helper()
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

// TestNOR210UnrelatedRelationshipTypeCacheSurvivesPrefixDrop is the reported
// defect: dropping one prefix evicted the warm relationship-type result of an
// unrelated prefix, so the next unrelated read had to refill from storage.
// Covered for a whole-database prefix and for a partial prefix inside one
// database, both of which only remove the victim's relationships.
func TestNOR210UnrelatedRelationshipTypeCacheSurvivesPrefixDrop(t *testing.T) {
	for _, prefix := range []string{"victim:", "victim:edge-"} {
		t.Run(prefix, func(t *testing.T) {
			engine := newTestEngine(t)
			seedDropCacheRelationshipType(t, engine, "victim", dropCacheVictimType, 2)
			seedDropCacheRelationshipType(t, engine, "peer", dropCachePeerType, 2)

			victimIDs := warmDropCacheRelationshipType(t, engine, dropCacheVictimType)
			require.Equal(t, []EdgeID{"victim:edge-0", "victim:edge-1"}, victimIDs)
			peerIDs := warmDropCacheRelationshipType(t, engine, dropCachePeerType)
			require.Equal(t, []EdgeID{"peer:edge-0", "peer:edge-1"}, peerIDs)
			require.Equal(t, peerIDs, cachedDropCacheRelationshipTypeIDs(t, engine, dropCachePeerType))
			require.NotEmpty(t, cachedDropCacheRelationshipTypeIDs(t, engine, dropCacheVictimType))

			// Capture the pre-drop state a read that raced the DROP would fill.
			victimEdges, err := engine.GetEdgesByType(context.Background(), dropCacheVictimType)
			require.NoError(t, err)
			gen, source := engine.edgeTypeCacheGen.current(), engine.db.oracle.published.Load()

			nodesDeleted, edgesDeleted, err := engine.DeleteByPrefix(prefix)
			require.NoError(t, err)
			require.EqualValues(t, 2, edgesDeleted)
			if prefix == "victim:" {
				require.EqualValues(t, 2, nodesDeleted)
			} else {
				require.Zero(t, nodesDeleted)
			}

			// The unrelated type keeps the entry that answers its next read.
			require.Equal(t, peerIDs, cachedDropCacheRelationshipTypeIDs(t, engine, dropCachePeerType),
				"dropping another prefix must retain the unrelated warm relationship-type result")
			require.Empty(t, cachedDropCacheRelationshipTypeIDs(t, engine, dropCacheVictimType),
				"the dropped type must not keep its pre-drop result")

			// A fill captured before the DROP cannot put the dropped result back.
			engine.edgeTypeCacheFill(gen, dropCacheVictimType, victimEdges, source)
			require.Empty(t, cachedDropCacheRelationshipTypeIDs(t, engine, dropCacheVictimType),
				"a relationship-type fill captured before the DROP must not reinstall it")

			// The served facts follow the durable graph for both prefixes.
			require.Equal(t, peerIDs, warmDropCacheRelationshipType(t, engine, dropCachePeerType))
			require.Empty(t, warmDropCacheRelationshipType(t, engine, dropCacheVictimType))
			_, err = engine.GetEdge(context.Background(), "victim:edge-0")
			require.ErrorIs(t, err, ErrNotFound)
			_, err = engine.GetEdge(context.Background(), "peer:edge-0")
			require.NoError(t, err)
		})
	}
}

// TestNOR210SharedRelationshipTypeCacheRevokedByPrefixDrop covers the affected
// shared type: one type owns relationships inside and outside the dropped
// prefix, so its whole-type result must be revoked rather than kept warm.
func TestNOR210SharedRelationshipTypeCacheRevokedByPrefixDrop(t *testing.T) {
	for _, prefix := range []string{"victim:", "victim:edge-"} {
		t.Run(prefix, func(t *testing.T) {
			engine := newTestEngine(t)
			seedDropCacheRelationshipType(t, engine, "victim", dropCacheSharedType, 2)
			seedDropCacheRelationshipType(t, engine, "peer", dropCacheSharedType, 2)

			require.Equal(t, []EdgeID{"peer:edge-0", "peer:edge-1", "victim:edge-0", "victim:edge-1"},
				warmDropCacheRelationshipType(t, engine, dropCacheSharedType))
			require.NotEmpty(t, cachedDropCacheRelationshipTypeIDs(t, engine, dropCacheSharedType))

			_, edgesDeleted, err := engine.DeleteByPrefix(prefix)
			require.NoError(t, err)
			require.EqualValues(t, 2, edgesDeleted)

			cached := cachedDropCacheRelationshipTypeIDs(t, engine, dropCacheSharedType)
			require.NotContains(t, cached, EdgeID("victim:edge-0"), "a shared type's result must not keep a dropped relationship")
			require.NotContains(t, cached, EdgeID("victim:edge-1"), "a shared type's result must not keep a dropped relationship")
			require.Equal(t, []EdgeID{"peer:edge-0", "peer:edge-1"}, warmDropCacheRelationshipType(t, engine, dropCacheSharedType))
		})
	}
}

// TestNOR210InterruptedPrefixDropKeepsRelationshipTypeRecovery covers the
// interrupted DROP: its rows are gone from the publication cut and its cleanup
// failed. The removed type is already revoked and the unrelated type keeps its
// valid warm result. A whole-database DROP holds its durable fence, so the
// engine-global relationship-type read stays fenced (fail closed, never a stale
// population) until the same DROP owner finishes the retry; a partial prefix
// holds no fence and serves the post-drop facts directly.
func TestNOR210InterruptedPrefixDropKeepsRelationshipTypeRecovery(t *testing.T) {
	for _, tc := range []struct {
		name   string
		prefix string
		fenced bool
	}{
		{name: "whole-namespace", prefix: "victim:", fenced: true},
		{name: "partial-prefix", prefix: "victim:edge-"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			engine := newTestEngine(t)
			seedDropCacheRelationshipType(t, engine, "victim", dropCacheVictimType, 2)
			seedDropCacheRelationshipType(t, engine, "peer", dropCachePeerType, 2)
			require.Equal(t, []EdgeID{"victim:edge-0", "victim:edge-1"}, warmDropCacheRelationshipType(t, engine, dropCacheVictimType))
			peerIDs := warmDropCacheRelationshipType(t, engine, dropCachePeerType)
			require.Equal(t, peerIDs, cachedDropCacheRelationshipTypeIDs(t, engine, dropCachePeerType))
			require.NotEmpty(t, cachedDropCacheRelationshipTypeIDs(t, engine, dropCacheVictimType))

			sentinel := errors.New("intrinsic DROP cleanup unavailable")
			engine.dropPrefixTailHook = func(prefix string) error {
				if prefix == tc.prefix {
					return sentinel
				}
				return nil
			}
			t.Cleanup(func() { engine.dropPrefixTailHook = nil })

			_, _, err := engine.DeleteByPrefix(tc.prefix)
			require.ErrorIs(t, err, sentinel)

			require.Empty(t, cachedDropCacheRelationshipTypeIDs(t, engine, dropCacheVictimType),
				"the interrupted DROP already published its row removal")
			require.Equal(t, peerIDs, cachedDropCacheRelationshipTypeIDs(t, engine, dropCachePeerType),
				"an unrelated type keeps its valid warm result across the interruption")

			if tc.fenced {
				require.ErrorIs(t, engine.pendingNamespaceConflict("victim"), ErrConflict, "the durable DROP fence is held")
				_, err = engine.GetEdgesByType(ctx, dropCachePeerType)
				require.ErrorIs(t, err, ErrConflict,
					"an engine-global relationship-type read is fenced by the pending DROP, cached or not")
			} else {
				require.NoError(t, engine.pendingNamespaceConflict("victim"), "a partial prefix drop holds no namespace fence")
				require.Equal(t, peerIDs, warmDropCacheRelationshipType(t, engine, dropCachePeerType))
				require.Empty(t, warmDropCacheRelationshipType(t, engine, dropCacheVictimType),
					"the interrupted DROP must not serve a dropped relationship")
			}

			// The same DROP owner finishes the interrupted work.
			engine.dropPrefixTailHook = nil
			_, _, err = engine.DeleteByPrefix(tc.prefix)
			require.NoError(t, err)
			require.NoError(t, engine.pendingNamespaceConflict("victim"))

			require.Equal(t, peerIDs, warmDropCacheRelationshipType(t, engine, dropCachePeerType))
			require.Empty(t, warmDropCacheRelationshipType(t, engine, dropCacheVictimType))
		})
	}
}
