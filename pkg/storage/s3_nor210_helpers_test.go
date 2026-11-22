package storage

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func s3SeedRelationshipType(tb testing.TB, engine *BadgerEngine, namespace, edgeType string, edges int) {
	tb.Helper()
	view := NewNamespacedEngine(engine, namespace)
	ctx := context.Background()
	for _, id := range []NodeID{"start", "end"} {
		_, err := view.CreateNode(ctx, &Node{ID: id, Labels: []string{"Endpoint"}})
		require.NoError(tb, err)
	}
	for i := 0; i < edges; i++ {
		err := view.CreateEdge(ctx, &Edge{ID: EdgeID(fmt.Sprintf("edge-%d", i)), StartNode: "start", EndNode: "end", Type: edgeType})
		require.NoError(tb, err)
	}
}

func s3WarmRelationshipType(tb testing.TB, view *NamespacedEngine, edgeType string, want int) []*Edge {
	tb.Helper()
	edges, err := view.GetEdgesByType(context.Background(), edgeType)
	require.NoError(tb, err)
	require.Len(tb, edges, want)
	return edges
}

func s3RelationshipTypeCacheEntryCount(engine *BadgerEngine) int {
	engine.edgeTypeCacheMu.RLock()
	defer engine.edgeTypeCacheMu.RUnlock()
	return len(engine.edgeTypeCache)
}

func s3CachedRelationshipTypeIDs(engine *BadgerEngine, edgeType string) []EdgeID {
	engine.edgeTypeCacheMu.RLock()
	defer engine.edgeTypeCacheMu.RUnlock()
	ids := make([]EdgeID, 0, len(engine.edgeTypeCache[edgeType]))
	for _, edge := range engine.edgeTypeCache[edgeType] {
		if edge != nil {
			ids = append(ids, edge.ID)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func s3AuditSortedEdgeIDs(edges []*Edge) []EdgeID {
	ids := make([]EdgeID, 0, len(edges))
	for _, edge := range edges {
		ids = append(ids, edge.ID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
