package storage

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type observedRelationshipProperty struct{ copies *atomic.Int64 }

func (value observedRelationshipProperty) CallerCopy() any { value.copies.Add(1); return value }

func TestAdjacencyMetadataDoesNotMaterializeUnreturnedProperties(t *testing.T) {
	store := NewInMemoryStorage()
	root, err := store.CreateNode(t.Context(), nil, nil)
	require.NoError(t, err)
	var end int64
	var copies atomic.Int64
	for range 16 {
		node, err := store.CreateNode(t.Context(), nil, nil)
		require.NoError(t, err)
		_, err = store.CreateRelationship(t.Context(), root.ID, node.ID, "LINK", map[string]any{"observed": observedRelationshipProperty{&copies}})
		require.NoError(t, err)
		end = node.ID
	}
	copies.Store(0)
	degree, err := store.GetNodeDegree(t.Context(), root.ID, "LINK", DirectionBoth)
	require.NoError(t, err)
	require.Equal(t, 16, degree)
	require.Zero(t, copies.Load(), "counting relationships must not copy their property payloads")
	path, err := store.FindShortestPath(t.Context(), root.ID, end, "LINK", 2)
	require.NoError(t, err)
	require.Equal(t, 1, path.Length)
	require.Equal(t, int64(1), copies.Load(), "only the returned path relationship needs a caller-owned payload")
	copies.Store(0)
	relationships, err := store.GetNodeRelationships(t.Context(), root.ID, "LINK", DirectionBoth)
	require.NoError(t, err)
	require.Len(t, relationships, 16)
	require.Equal(t, int64(16), copies.Load(), "the public full-record read remains detached")
}
