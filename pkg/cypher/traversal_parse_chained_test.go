package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestParseTraversalPattern_Chained_NorthwindSupplierCategory(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	pattern := "(s:Supplier)-[:SUPPLIES]->(p:Product)-[:PART_OF]->(c:Category)"
	m := exec.parseTraversalPattern(ctx, pattern)
	require.NotNil(t, m)
	require.True(t, m.IsChained, "expected chained pattern")
	require.Len(t, m.Segments, 2)
	require.Equal(t, "s", m.Segments[0].FromNode.variable)
	require.Equal(t, "p", m.Segments[0].ToNode.variable)
	require.Equal(t, "c", m.Segments[1].ToNode.variable)
	require.Equal(t, "outgoing", m.Segments[0].Relationship.Direction)
	require.Equal(t, "outgoing", m.Segments[1].Relationship.Direction)
}
