package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The audit file characterizes today's global eviction as a passing diagnostic.
// This separate red witness asserts NOR-210's original retention requirement,
// reusing the same real-storage fixture and current cache owner introspection.
func TestNOR210UnrelatedRelationshipTypeCacheSurvivesPrefixDrop(t *testing.T) {
	for _, prefix := range []string{"victim:", "victim:edge-"} {
		t.Run(prefix, func(t *testing.T) {
			engine := newTestEngine(t)
			s3SeedRelationshipType(t, engine, "victim", "NOR210_VICTIM", 2)
			s3SeedRelationshipType(t, engine, "peer", "NOR210_KEEP", 2)
			victim := NewNamespacedEngine(engine, "victim")
			peer := NewNamespacedEngine(engine, "peer")
			s3WarmRelationshipType(t, victim, "NOR210_VICTIM", 2)
			beforePublic := s3AuditSortedEdgeIDs(s3WarmRelationshipType(t, peer, "NOR210_KEEP", 2))
			before := s3CachedRelationshipTypeIDs(engine, "NOR210_KEEP")
			require.Len(t, before, 2)
			require.Equal(t, 2, s3RelationshipTypeCacheEntryCount(engine))
			_, removedEdges, err := engine.DeleteByPrefix(prefix)
			require.NoError(t, err)
			require.EqualValues(t, 2, removedEdges)
			// Inspect before any read can refill the evicted entry.
			assert.Equal(t, before, s3CachedRelationshipTypeIDs(engine, "NOR210_KEEP"),
				"dropping another prefix must retain this unrelated valid warm relationship-type entry")
			actualPeer, err := peer.GetEdgesByType(t.Context(), "NOR210_KEEP")
			require.NoError(t, err)
			require.Equal(t, beforePublic, s3AuditSortedEdgeIDs(actualPeer), "unrelated durable graph facts remain correct")
			actualVictim, err := victim.GetEdgesByType(t.Context(), "NOR210_VICTIM")
			require.NoError(t, err)
			require.Empty(t, actualVictim, "dropped relationships must not survive in a stale cache")
		})
	}
}
