package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupRemoveRelationshipFixture(t *testing.T) *StorageExecutor {
	t.Helper()
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "remove_rel_scope")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
		CREATE (a:Item {id: 'a', flag: true})
		CREATE (b:Item {id: 'b', flag: true})
		CREATE (a)-[:LINK {flag: true, keep: 'edge-only'}]->(b)
	`, nil)
	require.NoError(t, err)

	return exec
}

func setupTypedUndirectedRelationshipFixture(t *testing.T) *StorageExecutor {
	t.Helper()
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "typed_undirected_rel")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (:Item {name: 'orphan'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
		CREATE (c:Item {name: 'contains-start'})
		CREATE (p:Item {name: 'contains-end'})
		CREATE (c)-[:CONTAINS]->(p)
	`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
		CREATE (l:Item {name: 'likes-start'})
		CREATE (m:Item {name: 'likes-end'})
		CREATE (l)-[:LIKES]->(m)
	`, nil)
	require.NoError(t, err)

	return exec
}

func setupOptionalMatchProbeFixture(t *testing.T) *StorageExecutor {
	t.Helper()
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "optional_match_probe")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
		CREATE (n:Node {id: 'parent-node-id'})
		CREATE (c:NodeChunk:Node {id: 'chunk-1'})
		CREATE (n)-[:HAS_CHUNK {index: 0}]->(c)
	`, nil)
	require.NoError(t, err)

	return exec
}

func itemNames(rows [][]interface{}) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		name, ok := row[0].(string)
		if ok {
			out = append(out, name)
		}
	}
	return out
}

func TestRegression_RemoveRelationshipPropertyIsScopedToRelationship(t *testing.T) {
	exec := setupRemoveRelationshipFixture(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `MATCH (a:Item {id: 'a'})-[r:LINK]->(b:Item {id: 'b'}) REMOVE r.flag`, nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
		MATCH (a:Item {id: 'a'})-[r:LINK]->(b:Item {id: 'b'})
		RETURN a.flag, r.flag, b.flag, r.keep
	`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Len(t, result.Rows[0], 4)
	assert.Equal(t, true, result.Rows[0][0], "REMOVE r.flag must not touch the source node")
	assert.Nil(t, result.Rows[0][1], "REMOVE r.flag must remove the property from the relationship")
	assert.Equal(t, true, result.Rows[0][2], "REMOVE r.flag must not touch the target node")
	assert.Equal(t, "edge-only", result.Rows[0][3], "unrelated relationship properties must survive")
}

func TestRegression_TypedUndirectedRelationshipExistenceRespectsRelationshipType(t *testing.T) {
	exec := setupTypedUndirectedRelationshipFixture(t)
	ctx := context.Background()

	t.Run("WHERE typed undirected pattern filters by type", func(t *testing.T) {
		result, err := exec.Execute(ctx, `MATCH (n:Item) WHERE (n)-[:CONTAINS]-() RETURN n.name ORDER BY n.name`, nil)
		require.NoError(t, err)
		assert.Equal(t, []string{"contains-end", "contains-start"}, itemNames(result.Rows))
	})

	t.Run("EXISTS typed undirected subquery filters by type", func(t *testing.T) {
		result, err := exec.Execute(ctx, `MATCH (n:Item) WHERE EXISTS { MATCH (n)-[:CONTAINS]-() } RETURN n.name ORDER BY n.name`, nil)
		require.NoError(t, err)
		assert.Equal(t, []string{"contains-end", "contains-start"}, itemNames(result.Rows))
	})

	t.Run("COUNT typed undirected subquery filters by type", func(t *testing.T) {
		result, err := exec.Execute(ctx, `MATCH (n:Item) WHERE COUNT { MATCH (n)-[:CONTAINS]-() } = 1 RETURN n.name ORDER BY n.name`, nil)
		require.NoError(t, err)
		assert.Equal(t, []string{"contains-end", "contains-start"}, itemNames(result.Rows))
	})
}

func TestRegression_ExecuteMatchEmbeddedOptionalMatchProjectsRelationshipVariable(t *testing.T) {
	exec := setupOptionalMatchProbeFixture(t)
	ctx := context.Background()

	result, err := exec.executeMatch(ctx, `MATCH (n:Node {id: 'parent-node-id'}) OPTIONAL MATCH (n)-[r:HAS_CHUNK]->(chunk:NodeChunk) RETURN r, chunk`)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Len(t, result.Rows[0], 2)

	edge, ok := result.Rows[0][0].(*storage.Edge)
	require.True(t, ok, "embedded OPTIONAL MATCH should project the relationship variable as *storage.Edge, got %T", result.Rows[0][0])
	require.NotNil(t, edge)
	assert.Equal(t, storage.EdgeID("HAS_CHUNK"), storage.EdgeID(edge.Type), "relationship type should survive the probe path")
	assert.Equal(t, int64(0), edge.Properties["index"], "relationship properties should survive the probe path")

	node, ok := result.Rows[0][1].(*storage.Node)
	require.True(t, ok, "embedded OPTIONAL MATCH should project the optional node as *storage.Node, got %T", result.Rows[0][1])
	require.NotNil(t, node)
	assert.Equal(t, "chunk-1", node.Properties["id"])
}
