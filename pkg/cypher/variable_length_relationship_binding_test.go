package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestVariableLengthRelationshipBindingIsAListForListFunctions(t *testing.T) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "variable-length-list")
	executor := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := executor.Execute(ctx, `
		CREATE (a), (b), (c)
		CREATE (a)-[:T]->(b)
	`, nil)
	require.NoError(t, err)

	result, err := executor.Execute(ctx, `
		MATCH ()-[relationships*0..1]-()
		RETURN last(relationships) AS relationship
	`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 5)

	relationships, nulls := 0, 0
	for _, row := range result.Rows {
		switch row[0].(type) {
		case *storage.Edge:
			relationships++
		case nil:
			nulls++
		default:
			t.Fatalf("expected relationship or null, got %T", row[0])
		}
	}
	require.Equal(t, 2, relationships)
	require.Equal(t, 3, nulls)
}

func TestOptionalVariableLengthMatchRetainsRelationshipListAndNamedPath(t *testing.T) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "optional-variable-length-path")
	executor := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := executor.Execute(ctx, `
		CREATE (a {name: 'A'}), (b {name: 'B'}), (c {name: 'C'})
		CREATE (a)-[:X]->(b)
	`, nil)
	require.NoError(t, err)

	result, err := executor.Execute(ctx, `
		MATCH (a {name: 'A'}), (x)
		WHERE x.name IN ['B', 'C']
		OPTIONAL MATCH path = (a)-[relationships*]->(x)
		RETURN relationships, x, path
	`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 2)

	for _, row := range result.Rows {
		node, ok := row[1].(*storage.Node)
		require.True(t, ok)
		switch node.Properties["name"] {
		case "B":
			require.Len(t, toAnySlice(row[0]), 1)
			path := requireReturnedPath(t, row[2])
			require.Len(t, path.Nodes, 2)
			require.Len(t, path.Relationships, 1)
		case "C":
			require.Nil(t, row[0])
			require.Nil(t, row[2])
		default:
			t.Fatalf("unexpected node %v", node.Properties["name"])
		}
	}
}
