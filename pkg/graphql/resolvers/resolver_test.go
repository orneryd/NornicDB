package resolvers

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/orneryd/nornicdb/pkg/auth"
	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/graphql/models"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testDB creates a temporary NornicDB instance for testing
func testDB(t *testing.T) *nornicdb.DB {
	t.Helper()
	db, err := nornicdb.Open(t.TempDir(), &nornicdb.Config{
		Memory: nornicConfig.MemoryConfig{
			DecayEnabled:     false,
			AutoLinksEnabled: false,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

// testDBManager creates a DatabaseManager for testing
func testDBManager(t *testing.T, db *nornicdb.DB) *multidb.DatabaseManager {
	t.Helper()
	// Get the base storage (unwraps NamespacedEngine) - DatabaseManager creates its own NamespacedEngines
	inner := db.GetBaseStorageForManager()
	manager, err := multidb.NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	t.Cleanup(func() { manager.Close() })
	return manager
}

// createNodeViaCypher creates a node via Cypher query (namespaced)
func createNodeViaCypher(t *testing.T, resolver *Resolver, labels []string, properties map[string]interface{}) *nornicdb.Node {
	t.Helper()
	ctx := context.Background()

	// Build labels string
	labelsStr := ""
	if len(labels) > 0 {
		labelsStr = ":" + labels[0]
		for i := 1; i < len(labels); i++ {
			labelsStr += ":" + labels[i]
		}
	}

	// Build properties
	propsStr := "{"
	params := make(map[string]interface{})
	first := true
	i := 0
	for k, v := range properties {
		if !first {
			propsStr += ", "
		}
		first = false
		paramName := fmt.Sprintf("p%d", i)
		propsStr += fmt.Sprintf("%s: $%s", k, paramName)
		params[paramName] = v
		i++
	}
	propsStr += "}"

	query := fmt.Sprintf("CREATE (n%s %s) RETURN n", labelsStr, propsStr)
	result, err := resolver.executeCypher(ctx, query, params, "", false)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	node, err := extractNodeFromResult(result.Rows[0][0])
	require.NoError(t, err)
	return node
}

// createEdgeViaCypher creates an edge via Cypher query (namespaced)
func createEdgeViaCypher(t *testing.T, resolver *Resolver, sourceID, targetID, edgeType string, properties map[string]interface{}) *nornicdb.GraphEdge {
	t.Helper()
	ctx := context.Background()

	// Use the resolver's createEdgeViaCypher function which handles the query correctly
	edge, err := resolver.createEdgeViaCypher(ctx, sourceID, targetID, edgeType, properties)
	require.NoError(t, err, "failed to create relationship: source=%s, target=%s", sourceID, targetID)
	return edge
}

func TestNewResolver(t *testing.T) {
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)

	assert.NotNil(t, resolver)
	assert.Equal(t, db, resolver.DB)
	assert.NotNil(t, resolver.dbManager)
	assert.False(t, resolver.StartTime.IsZero())
}

func TestNewResolver_RequiresDBManager(t *testing.T) {
	db := testDB(t)

	// Should panic if dbManager is nil
	assert.Panics(t, func() {
		NewResolver(db, nil)
	})
}

// =============================================================================
// Query Tests
// =============================================================================

func TestQueryNode(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	qr := &queryResolver{resolver}

	t.Run("returns nil for non-existent node", func(t *testing.T) {
		node, err := qr.queryNode(ctx, "non-existent-id")
		assert.NoError(t, err)
		assert.Nil(t, node)
	})

	t.Run("returns node by ID", func(t *testing.T) {
		// Create a node via Cypher (namespaced)
		createdNode := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{
			"name": "Alice",
			"age":  30,
		})

		// Query it
		node, err := qr.queryNode(ctx, createdNode.ID)
		assert.NoError(t, err)
		require.NotNil(t, node)
		assert.Equal(t, createdNode.ID, node.ID)
		assert.Contains(t, node.Labels, "Person")
		assert.Equal(t, "Alice", node.Properties["name"])
	})
}

func TestGetCypherExecutor_WiresDatabaseManager(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)

	exec, err := resolver.getCypherExecutor(ctx, "")
	require.NoError(t, err)

	// SHOW DATABASES requires dbManager wiring in cypher executor.
	result, err := exec.Execute(ctx, "SHOW DATABASES", nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotEmpty(t, result.Rows)
}

func TestQueryNodes(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	qr := &queryResolver{resolver}

	t.Run("returns empty for non-existent IDs", func(t *testing.T) {
		nodes, err := qr.queryNodes(ctx, []string{"id1", "id2"})
		assert.NoError(t, err)
		assert.Empty(t, nodes)
	})

	t.Run("returns existing nodes", func(t *testing.T) {
		// Create nodes
		n1 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		n2 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Bob"})

		nodes, err := qr.queryNodes(ctx, []string{n1.ID, n2.ID, "non-existent"})
		assert.NoError(t, err)
		assert.Len(t, nodes, 2)
	})
}

func TestQueryAllNodes(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	qr := &queryResolver{resolver}

	t.Run("returns empty when no nodes", func(t *testing.T) {
		nodes, err := qr.queryAllNodes(ctx, nil, nil, nil)
		assert.NoError(t, err)
		assert.Empty(t, nodes)
	})

	t.Run("returns all nodes with limit", func(t *testing.T) {
		// Create nodes
		createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Bob"})
		createNodeViaCypher(t, resolver, []string{"Company"}, map[string]interface{}{"name": "TechCorp"})

		limit := 10
		nodes, err := qr.queryAllNodes(ctx, nil, &limit, nil)
		assert.NoError(t, err)
		assert.Len(t, nodes, 3)
	})

	t.Run("filters by label", func(t *testing.T) {
		nodes, err := qr.queryAllNodes(ctx, []string{"Person"}, nil, nil)
		assert.NoError(t, err)
		assert.Len(t, nodes, 2)
		for _, n := range nodes {
			assert.Contains(t, n.Labels, "Person")
		}
	})

	t.Run("supports pagination", func(t *testing.T) {
		limit := 1
		offset := 1
		nodes, err := qr.queryAllNodes(ctx, nil, &limit, &offset)
		assert.NoError(t, err)
		assert.Len(t, nodes, 1)
	})
}

func TestQueryNodeCount(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	qr := &queryResolver{resolver}

	t.Run("returns 0 when empty", func(t *testing.T) {
		count, err := qr.queryNodeCount(ctx, nil)
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("returns total count", func(t *testing.T) {
		createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		createNodeViaCypher(t, resolver, []string{"Company"}, map[string]interface{}{"name": "TechCorp"})

		count, err := qr.queryNodeCount(ctx, nil)
		assert.NoError(t, err)
		assert.Equal(t, 2, count)
	})

	t.Run("counts by label", func(t *testing.T) {
		label := "Person"
		count, err := qr.queryNodeCount(ctx, &label)
		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})
}

func TestQueryRelationship(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	qr := &queryResolver{resolver}

	t.Run("returns nil for non-existent relationship", func(t *testing.T) {
		rel, err := qr.queryRelationship(ctx, "non-existent")
		assert.NoError(t, err)
		assert.Nil(t, rel)
	})

	t.Run("returns relationship by ID", func(t *testing.T) {
		// Create nodes and relationship
		n1 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		n2 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Bob"})
		edge := createEdgeViaCypher(t, resolver, n1.ID, n2.ID, "KNOWS", map[string]interface{}{"since": "2020"})

		rel, err := qr.queryRelationship(ctx, edge.ID)
		assert.NoError(t, err)
		require.NotNil(t, rel)
		assert.Equal(t, edge.ID, rel.ID)
		assert.Equal(t, "KNOWS", rel.Type)
	})
}

func TestQueryRelationshipsBetween(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	qr := &queryResolver{resolver}

	t.Run("returns relationships between nodes", func(t *testing.T) {
		n1 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		n2 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Bob"})
		createEdgeViaCypher(t, resolver, n1.ID, n2.ID, "KNOWS", nil)
		createEdgeViaCypher(t, resolver, n1.ID, n2.ID, "WORKS_WITH", nil)

		rels, err := qr.queryRelationshipsBetween(ctx, n1.ID, n2.ID)
		assert.NoError(t, err)
		assert.Len(t, rels, 2)
	})
}

func TestQueryStats(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	qr := &queryResolver{resolver}

	t.Run("returns database statistics", func(t *testing.T) {
		createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Bob"})

		stats, err := qr.queryStats(ctx)
		assert.NoError(t, err)
		require.NotNil(t, stats)
		assert.Equal(t, 2, stats.NodeCount)
		assert.GreaterOrEqual(t, stats.UptimeSeconds, 0.0)
	})
}

func TestQueryLabels(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	qr := &queryResolver{resolver}

	t.Run("returns all labels", func(t *testing.T) {
		createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		createNodeViaCypher(t, resolver, []string{"Company"}, map[string]interface{}{"name": "TechCorp"})

		labels, err := qr.queryLabels(ctx)
		assert.NoError(t, err)
		assert.Contains(t, labels, "Person")
		assert.Contains(t, labels, "Company")
	})
}

func TestQueryCypher(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	qr := &queryResolver{resolver}

	t.Run("executes cypher query", func(t *testing.T) {
		createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Alice"})

		input := models.CypherInput{
			Statement:  "MATCH (n:Person) RETURN n.name as name",
			Parameters: nil,
		}
		result, err := qr.queryCypher(ctx, input)
		assert.NoError(t, err)
		require.NotNil(t, result)
		assert.Contains(t, result.Columns, "name")
		assert.Equal(t, 1, result.RowCount)
	})

	t.Run("supports parameters", func(t *testing.T) {
		input := models.CypherInput{
			Statement:  "MATCH (n:Person) WHERE n.name = $name RETURN n",
			Parameters: models.JSON{"name": "Alice"},
		}
		result, err := qr.queryCypher(ctx, input)
		assert.NoError(t, err)
		assert.Equal(t, 1, result.RowCount)
	})
}

func TestQueryShortestPath(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	qr := &queryResolver{resolver}

	t.Run("finds shortest path", func(t *testing.T) {
		// Create a simple graph: A -> B -> C
		a := createNodeViaCypher(t, resolver, []string{"Node"}, map[string]interface{}{"name": "A"})
		b := createNodeViaCypher(t, resolver, []string{"Node"}, map[string]interface{}{"name": "B"})
		c := createNodeViaCypher(t, resolver, []string{"Node"}, map[string]interface{}{"name": "C"})
		createEdgeViaCypher(t, resolver, a.ID, b.ID, "CONNECTS", nil)
		createEdgeViaCypher(t, resolver, b.ID, c.ID, "CONNECTS", nil)

		path, err := qr.queryShortestPath(ctx, a.ID, c.ID, nil, nil)
		assert.NoError(t, err)
		assert.Len(t, path, 3) // A -> B -> C
	})

	t.Run("returns empty for disconnected nodes", func(t *testing.T) {
		a := createNodeViaCypher(t, resolver, []string{"Isolated"}, map[string]interface{}{"name": "X"})
		b := createNodeViaCypher(t, resolver, []string{"Isolated"}, map[string]interface{}{"name": "Y"})

		path, err := qr.queryShortestPath(ctx, a.ID, b.ID, nil, nil)
		assert.NoError(t, err)
		assert.Empty(t, path)
	})

	t.Run("respects relationship type filter and max depth", func(t *testing.T) {
		a := createNodeViaCypher(t, resolver, []string{"Node"}, map[string]interface{}{"name": "A2"})
		b := createNodeViaCypher(t, resolver, []string{"Node"}, map[string]interface{}{"name": "B2"})
		c := createNodeViaCypher(t, resolver, []string{"Node"}, map[string]interface{}{"name": "C2"})
		createEdgeViaCypher(t, resolver, a.ID, b.ID, "KNOWS", nil)
		createEdgeViaCypher(t, resolver, b.ID, c.ID, "WORKS_WITH", nil)

		depth := 1
		path, err := qr.queryShortestPath(ctx, a.ID, c.ID, &depth, []string{"KNOWS"})
		assert.NoError(t, err)
		assert.Empty(t, path)
	})
}

func TestQueryNeighborhood(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	qr := &queryResolver{resolver}

	t.Run("returns neighborhood subgraph", func(t *testing.T) {
		// Create a star graph: center connected to 3 nodes
		center := createNodeViaCypher(t, resolver, []string{"Center"}, map[string]interface{}{"name": "Hub"})
		n1 := createNodeViaCypher(t, resolver, []string{"Leaf"}, map[string]interface{}{"name": "L1"})
		n2 := createNodeViaCypher(t, resolver, []string{"Leaf"}, map[string]interface{}{"name": "L2"})
		n3 := createNodeViaCypher(t, resolver, []string{"Leaf"}, map[string]interface{}{"name": "L3"})
		createEdgeViaCypher(t, resolver, center.ID, n1.ID, "CONNECTS", nil)
		createEdgeViaCypher(t, resolver, center.ID, n2.ID, "CONNECTS", nil)
		createEdgeViaCypher(t, resolver, center.ID, n3.ID, "CONNECTS", nil)

		depth := 1
		subgraph, err := qr.queryNeighborhood(ctx, center.ID, &depth, nil, nil, nil)
		assert.NoError(t, err)
		require.NotNil(t, subgraph)
		assert.Len(t, subgraph.Nodes, 4)         // center + 3 leaves
		assert.Len(t, subgraph.Relationships, 3) // 3 edges
	})

	t.Run("filters by relationship type label and limit", func(t *testing.T) {
		center := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Center"})
		friend := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Friend"})
		company := createNodeViaCypher(t, resolver, []string{"Company"}, map[string]interface{}{"name": "Company"})
		createEdgeViaCypher(t, resolver, center.ID, friend.ID, "KNOWS", nil)
		createEdgeViaCypher(t, resolver, center.ID, company.ID, "WORKS_AT", nil)

		depth := 2
		limit := 2
		subgraph, err := qr.queryNeighborhood(ctx, center.ID, &depth, []string{"KNOWS"}, []string{"Person"}, &limit)
		require.NoError(t, err)
		require.NotNil(t, subgraph)
		assert.Len(t, subgraph.Nodes, 2)
		assert.Len(t, subgraph.Relationships, 1)
		assert.Equal(t, "KNOWS", subgraph.Relationships[0].Type)
		for _, node := range subgraph.Nodes {
			if node.ID != center.ID {
				assert.Contains(t, node.Labels, "Person")
			}
		}
	})
}

// =============================================================================
// Mutation Tests
// =============================================================================

func TestMutationCreateNode(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	mr := &mutationResolver{resolver}

	t.Run("creates node with labels and properties", func(t *testing.T) {
		input := models.CreateNodeInput{
			Labels: []string{"Person", "Employee"},
			Properties: models.JSON{
				"name":  "Alice",
				"age":   30,
				"email": "alice@example.com",
			},
		}

		node, err := mr.mutationCreateNode(ctx, input)
		assert.NoError(t, err)
		require.NotNil(t, node)
		assert.NotEmpty(t, node.ID)
		assert.Contains(t, node.Labels, "Person")
		assert.Contains(t, node.Labels, "Employee")
		assert.Equal(t, "Alice", node.Properties["name"])
	})
}

func TestMutationUpdateNode(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	mr := &mutationResolver{resolver}

	t.Run("updates node properties", func(t *testing.T) {
		// Create node first
		created := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{
			"name": "Alice",
			"age":  30,
		})

		input := models.UpdateNodeInput{
			ID: created.ID,
			Properties: models.JSON{
				"age":   31,
				"title": "Senior Engineer",
			},
		}

		updated, err := mr.mutationUpdateNode(ctx, input)
		assert.NoError(t, err)
		require.NotNil(t, updated)
		assert.EqualValues(t, 31, updated.Properties["age"])
		assert.Equal(t, "Senior Engineer", updated.Properties["title"])
	})
}

func TestMutationDeleteNode(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	mr := &mutationResolver{resolver}

	t.Run("deletes existing node", func(t *testing.T) {
		created := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "ToDelete"})

		success, err := mr.mutationDeleteNode(ctx, created.ID)
		assert.NoError(t, err)
		assert.True(t, success)

		// Verify deleted
		_, err = resolver.getNode(ctx, created.ID)
		assert.Error(t, err)
	})

	t.Run("returns error for non-existent node", func(t *testing.T) {
		_, err := mr.mutationDeleteNode(ctx, "non-existent-id")
		assert.Error(t, err)
	})
}

func TestMutationBulkCreateNodes(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	mr := &mutationResolver{resolver}

	t.Run("creates multiple nodes", func(t *testing.T) {
		input := models.BulkCreateNodesInput{
			Nodes: []*models.CreateNodeInput{
				{Labels: []string{"Person"}, Properties: models.JSON{"name": "Alice"}},
				{Labels: []string{"Person"}, Properties: models.JSON{"name": "Bob"}},
				{Labels: []string{"Person"}, Properties: models.JSON{"name": "Charlie"}},
			},
		}

		result, err := mr.mutationBulkCreateNodes(ctx, input)
		assert.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, 3, result.Created)
		assert.Equal(t, 0, result.Skipped)
		assert.Empty(t, result.Errors)
	})
}

func TestMutationBulkDeleteNodes(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	mr := &mutationResolver{resolver}

	t.Run("deletes multiple nodes", func(t *testing.T) {
		n1 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		n2 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Bob"})

		result, err := mr.mutationBulkDeleteNodes(ctx, []string{n1.ID, n2.ID, "non-existent"})
		assert.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, 2, result.Deleted)
		assert.Contains(t, result.NotFound, "non-existent")
	})
}

func TestMutationCreateRelationship(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	mr := &mutationResolver{resolver}

	t.Run("creates relationship between nodes", func(t *testing.T) {
		n1 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		n2 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Bob"})

		input := models.CreateRelationshipInput{
			StartNodeID: n1.ID,
			EndNodeID:   n2.ID,
			Type:        "KNOWS",
			Properties:  models.JSON{"since": "2020"},
		}

		rel, err := mr.mutationCreateRelationship(ctx, input)
		assert.NoError(t, err)
		require.NotNil(t, rel)
		assert.Equal(t, "KNOWS", rel.Type)
		assert.Equal(t, n1.ID, rel.StartNodeID)
		assert.Equal(t, n2.ID, rel.EndNodeID)
	})
}

func TestMutationDeleteRelationship(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	mr := &mutationResolver{resolver}

	t.Run("deletes existing relationship", func(t *testing.T) {
		n1 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		n2 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Bob"})
		edge := createEdgeViaCypher(t, resolver, n1.ID, n2.ID, "KNOWS", nil)

		success, err := mr.mutationDeleteRelationship(ctx, edge.ID)
		assert.NoError(t, err)
		assert.True(t, success)
	})
}

func TestMutationMergeNode(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	mr := &mutationResolver{resolver}

	t.Run("creates new node when not found", func(t *testing.T) {
		node, err := mr.mutationMergeNode(ctx,
			[]string{"Person"},
			models.JSON{"email": "new@example.com"},
			models.JSON{"name": "New User"},
		)
		assert.NoError(t, err)
		require.NotNil(t, node)
		assert.Equal(t, "new@example.com", node.Properties["email"])
		assert.Equal(t, "New User", node.Properties["name"])
	})

	t.Run("updates existing node when found", func(t *testing.T) {
		// Create initial node
		createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{
			"email":     "existing@example.com",
			"name":      "Old Name",
			"lastLogin": "2024-01-01",
		})

		node, err := mr.mutationMergeNode(ctx,
			[]string{"Person"},
			models.JSON{"email": "existing@example.com"},
			models.JSON{"lastLogin": "2024-12-16"},
		)
		assert.NoError(t, err)
		require.NotNil(t, node)
		assert.Equal(t, "2024-12-16", node.Properties["lastLogin"])
	})
}

func TestMutationExecuteCypher(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	mr := &mutationResolver{resolver}

	t.Run("executes create cypher", func(t *testing.T) {
		input := models.CypherInput{
			Statement:  "CREATE (n:Test {name: $name}) RETURN n",
			Parameters: models.JSON{"name": "TestNode"},
		}

		result, err := mr.mutationExecuteCypher(ctx, input)
		assert.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, 1, result.RowCount)
	})
}

func TestMutationClearAll(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	mr := &mutationResolver{resolver}

	t.Run("requires correct confirmation phrase", func(t *testing.T) {
		_, err := mr.mutationClearAll(ctx, "wrong phrase")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid confirmation phrase")
	})

	t.Run("clears all data with correct phrase", func(t *testing.T) {
		// Create some data
		createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Bob"})

		success, err := mr.mutationClearAll(ctx, "DELETE ALL DATA")
		assert.NoError(t, err)
		assert.True(t, success)

		// Verify cleared - check namespaced stats via query
		qr := &queryResolver{resolver}
		stats, err := qr.queryStats(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, stats.NodeCount)
	})
}

func TestMutationRebuildSearchIndex(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	mr := &mutationResolver{resolver}

	t.Run("rebuilds search index", func(t *testing.T) {
		success, err := mr.mutationRebuildSearchIndex(ctx)
		assert.NoError(t, err)
		assert.True(t, success)
	})
}

// =============================================================================
// Node Resolver Tests
// =============================================================================

func TestNodeRelationships(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	nr := &nodeResolver{resolver}

	t.Run("returns relationships for node", func(t *testing.T) {
		n1 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		n2 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Bob"})
		n3 := createNodeViaCypher(t, resolver, []string{"Company"}, map[string]interface{}{"name": "TechCorp"})
		createEdgeViaCypher(t, resolver, n1.ID, n2.ID, "KNOWS", nil)
		createEdgeViaCypher(t, resolver, n1.ID, n3.ID, "WORKS_AT", nil)

		node := &models.Node{ID: n1.ID}
		rels, err := nr.nodeRelationships(ctx, node, nil, nil, nil)
		assert.NoError(t, err)
		assert.Len(t, rels, 2)
	})

	t.Run("filters by type", func(t *testing.T) {
		n1 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Test"})
		n2 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Other"})
		createEdgeViaCypher(t, resolver, n1.ID, n2.ID, "KNOWS", nil)
		createEdgeViaCypher(t, resolver, n1.ID, n2.ID, "WORKS_WITH", nil)

		node := &models.Node{ID: n1.ID}
		rels, err := nr.nodeRelationships(ctx, node, []string{"KNOWS"}, nil, nil)
		assert.NoError(t, err)
		assert.Len(t, rels, 1)
		assert.Equal(t, "KNOWS", rels[0].Type)
	})

	t.Run("filters by direction", func(t *testing.T) {
		n1 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Center"})
		n2 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Out"})
		n3 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "In"})
		createEdgeViaCypher(t, resolver, n1.ID, n2.ID, "OUTGOING", nil) // outgoing from n1
		createEdgeViaCypher(t, resolver, n3.ID, n1.ID, "INCOMING", nil) // incoming to n1

		node := &models.Node{ID: n1.ID}
		outDir := models.RelationshipDirectionOutgoing
		rels, err := nr.nodeRelationships(ctx, node, nil, &outDir, nil)
		assert.NoError(t, err)
		assert.Len(t, rels, 1)
		assert.Equal(t, "OUTGOING", rels[0].Type)
	})
}

func TestNodeNeighbors(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	nr := &nodeResolver{resolver}

	t.Run("returns neighboring nodes", func(t *testing.T) {
		n1 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		n2 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Bob"})
		n3 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Charlie"})
		createEdgeViaCypher(t, resolver, n1.ID, n2.ID, "KNOWS", nil)
		createEdgeViaCypher(t, resolver, n1.ID, n3.ID, "KNOWS", nil)

		node := &models.Node{ID: n1.ID}
		neighbors, err := nr.nodeNeighbors(ctx, node, nil, nil, nil, nil)
		assert.NoError(t, err)
		assert.Len(t, neighbors, 2)
	})

	t.Run("filters by label", func(t *testing.T) {
		n1 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Center"})
		n2 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "PersonNeighbor"})
		n3 := createNodeViaCypher(t, resolver, []string{"Company"}, map[string]interface{}{"name": "CompanyNeighbor"})
		createEdgeViaCypher(t, resolver, n1.ID, n2.ID, "KNOWS", nil)
		createEdgeViaCypher(t, resolver, n1.ID, n3.ID, "WORKS_AT", nil)

		node := &models.Node{ID: n1.ID}
		neighbors, err := nr.nodeNeighbors(ctx, node, nil, nil, []string{"Person"}, nil)
		assert.NoError(t, err)
		assert.Len(t, neighbors, 1)
		assert.Contains(t, neighbors[0].Labels, "Person")
	})

	t.Run("filters by direction and limit", func(t *testing.T) {
		center := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Center"})
		out1 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Out1"})
		out2 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Out2"})
		in1 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "In1"})
		createEdgeViaCypher(t, resolver, center.ID, out1.ID, "KNOWS", nil)
		createEdgeViaCypher(t, resolver, center.ID, out2.ID, "KNOWS", nil)
		createEdgeViaCypher(t, resolver, in1.ID, center.ID, "KNOWS", nil)

		node := &models.Node{ID: center.ID}
		dir := models.RelationshipDirectionOutgoing
		limit := 1
		neighbors, err := nr.nodeNeighbors(ctx, node, &dir, []string{"KNOWS"}, nil, &limit)
		require.NoError(t, err)
		assert.Len(t, neighbors, 1)
		assert.NotEqual(t, "In1", neighbors[0].Properties["name"])
	})
}

// =============================================================================
// Relationship Resolver Tests
// =============================================================================

func TestRelationshipStartNode(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	rr := &relationshipResolver{resolver}

	t.Run("returns start node", func(t *testing.T) {
		n1 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		n2 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Bob"})
		createEdgeViaCypher(t, resolver, n1.ID, n2.ID, "KNOWS", nil)

		rel := &models.Relationship{StartNodeID: n1.ID, EndNodeID: n2.ID}
		startNode, err := rr.relationshipStartNode(ctx, rel)
		assert.NoError(t, err)
		require.NotNil(t, startNode)
		assert.Equal(t, n1.ID, startNode.ID)
	})
}

func TestRelationshipEndNode(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	rr := &relationshipResolver{resolver}

	t.Run("returns end node", func(t *testing.T) {
		n1 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		n2 := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Bob"})
		createEdgeViaCypher(t, resolver, n1.ID, n2.ID, "KNOWS", nil)

		rel := &models.Relationship{StartNodeID: n1.ID, EndNodeID: n2.ID}
		endNode, err := rr.relationshipEndNode(ctx, rel)
		assert.NoError(t, err)
		require.NotNil(t, endNode)
		assert.Equal(t, n2.ID, endNode.ID)
	})
}

// =============================================================================
// Helper Function Tests
// =============================================================================

func TestDbNodeToModel(t *testing.T) {
	t.Run("returns nil for nil input", func(t *testing.T) {
		result := dbNodeToModel(nil)
		assert.Nil(t, result)
	})

	t.Run("converts node correctly", func(t *testing.T) {
		now := time.Now()
		node := &nornicdb.Node{
			ID:        "test-id",
			Labels:    []string{"Person", "Employee"},
			CreatedAt: now,
			Properties: map[string]interface{}{
				"name": "Alice",
				"age":  30,
			},
		}

		result := dbNodeToModel(node)
		require.NotNil(t, result)
		assert.Equal(t, "test-id", result.ID)
		assert.Equal(t, "test-id", result.InternalID)
		assert.Equal(t, []string{"Person", "Employee"}, result.Labels)
		assert.Equal(t, "Alice", result.Properties["name"])
		assert.NotNil(t, result.CreatedAt)
	})
}

func TestDbEdgeToModel(t *testing.T) {
	t.Run("returns nil for nil input", func(t *testing.T) {
		result := dbEdgeToModel(nil)
		assert.Nil(t, result)
	})

	t.Run("converts edge correctly", func(t *testing.T) {
		now := time.Now()
		edge := &nornicdb.GraphEdge{
			ID:        "edge-id",
			Source:    "source-id",
			Target:    "target-id",
			Type:      "KNOWS",
			CreatedAt: now,
			Properties: map[string]interface{}{
				"since": "2020",
			},
		}

		result := dbEdgeToModel(edge)
		require.NotNil(t, result)
		assert.Equal(t, "edge-id", result.ID)
		assert.Equal(t, "source-id", result.StartNodeID)
		assert.Equal(t, "target-id", result.EndNodeID)
		assert.Equal(t, "KNOWS", result.Type)
		assert.Equal(t, "2020", result.Properties["since"])
	})
}

func TestGraphQLQueryAdditionalCoverage(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	qr := &queryResolver{resolver}

	alice := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Alice", "role": "Engineer"})
	bob := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Bob", "role": "Engineer"})
	company := createNodeViaCypher(t, resolver, []string{"Company"}, map[string]interface{}{"name": "Acme"})
	knows := createEdgeViaCypher(t, resolver, alice.ID, bob.ID, "KNOWS", map[string]interface{}{"since": "2020"})
	createEdgeViaCypher(t, resolver, alice.ID, company.ID, "WORKS_AT", map[string]interface{}{"since": "2021"})

	t.Run("query nodes by label and relationships by type", func(t *testing.T) {
		nodes, err := qr.queryNodesByLabel(ctx, "Person", nil, nil)
		require.NoError(t, err)
		assert.Len(t, nodes, 2)

		rels, err := qr.queryAllRelationships(ctx, []string{"KNOWS"}, nil, nil)
		require.NoError(t, err)
		assert.Len(t, rels, 1)
		assert.Equal(t, "KNOWS", rels[0].Type)

		rels, err = qr.queryRelationshipsByType(ctx, "WORKS_AT", nil, nil)
		require.NoError(t, err)
		assert.Len(t, rels, 1)
		assert.Equal(t, "WORKS_AT", rels[0].Type)
	})

	t.Run("relationship count schema and relationship types", func(t *testing.T) {
		total, err := qr.queryRelationshipCount(ctx, nil)
		require.NoError(t, err)
		assert.Equal(t, 2, total)

		typ := "KNOWS"
		byType, err := qr.queryRelationshipCount(ctx, &typ)
		require.NoError(t, err)
		assert.Equal(t, 1, byType)

		schema, err := qr.querySchema(ctx)
		require.NoError(t, err)
		assert.Contains(t, schema.NodeLabels, "Person")
		assert.Contains(t, schema.RelationshipTypes, "KNOWS")

		types, err := qr.queryRelationshipTypes(ctx)
		require.NoError(t, err)
		assert.Contains(t, types, "KNOWS")
		assert.Contains(t, types, "WORKS_AT")
	})

	t.Run("search and search by property", func(t *testing.T) {
		searchResp, err := qr.querySearch(ctx, "Alice", &models.SearchOptions{})
		require.NoError(t, err)
		require.NotNil(t, searchResp)
		assert.GreaterOrEqual(t, searchResp.TotalCount, 0)

		propertyMatches, err := qr.querySearchByProperty(ctx, "name", models.JSON{"value": "Alice"}, nil, nil)
		require.NoError(t, err)
		assert.NotNil(t, propertyMatches)
	})

	t.Run("all paths wraps shortest path result", func(t *testing.T) {
		paths, err := qr.queryAllPaths(ctx, alice.ID, bob.ID, nil, nil)
		require.NoError(t, err)
		require.Len(t, paths, 1)
		assert.Len(t, paths[0], 2)

		isolated := createNodeViaCypher(t, resolver, []string{"Isolated"}, map[string]interface{}{"name": "Solo"})
		emptyPaths, err := qr.queryAllPaths(ctx, bob.ID, isolated.ID, nil, nil)
		require.NoError(t, err)
		assert.Empty(t, emptyPaths)
	})

	t.Run("query relationship returns created edge", func(t *testing.T) {
		rel, err := qr.queryRelationship(ctx, knows.ID)
		require.NoError(t, err)
		require.NotNil(t, rel)
		assert.Equal(t, knows.ID, rel.ID)
	})
}

func TestGraphQLMutationAdditionalCoverage(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	mr := &mutationResolver{resolver}

	t.Run("update relationship bulk operations and merge relationship", func(t *testing.T) {
		start := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Start"})
		mid := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Mid"})
		end := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "End"})
		edge := createEdgeViaCypher(t, resolver, start.ID, mid.ID, "KNOWS", map[string]interface{}{"since": "2020"})

		updated, err := mr.mutationUpdateRelationship(ctx, models.UpdateRelationshipInput{
			ID:         edge.ID,
			Properties: models.JSON{"since": "2025", "strength": "high"},
		})
		require.NoError(t, err)
		require.NotNil(t, updated)
		assert.Equal(t, "high", updated.Properties["strength"])

		bulkCreate, err := mr.mutationBulkCreateRelationships(ctx, models.BulkCreateRelationshipsInput{
			Relationships: []*models.CreateRelationshipInput{
				{StartNodeID: start.ID, EndNodeID: end.ID, Type: "WORKS_WITH", Properties: models.JSON{"active": true}},
				{StartNodeID: start.ID, EndNodeID: "missing-node", Type: "BROKEN"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, 1, bulkCreate.Created)
		assert.Equal(t, 1, bulkCreate.Skipped)
		assert.Len(t, bulkCreate.Errors, 1)

		merged, err := mr.mutationMergeRelationship(ctx, start.ID, end.ID, "WORKS_WITH", models.JSON{"active": false, "weight": 2})
		require.NoError(t, err)
		require.NotNil(t, merged)
		assert.Equal(t, false, merged.Properties["active"])
		assert.EqualValues(t, 2, merged.Properties["weight"])

		created, err := mr.mutationMergeRelationship(ctx, mid.ID, end.ID, "KNOWS", models.JSON{"since": "2024"})
		require.NoError(t, err)
		require.NotNil(t, created)
		assert.Equal(t, "KNOWS", created.Type)

		bulkDelete, err := mr.mutationBulkDeleteRelationships(ctx, []string{updated.ID, "missing-relationship"})
		require.NoError(t, err)
		assert.Equal(t, 2, bulkDelete.Deleted)
		assert.Empty(t, bulkDelete.NotFound)
	})

	t.Run("trigger embedding and run decay paths", func(t *testing.T) {
		status, err := mr.mutationTriggerEmbedding(ctx, nil)
		require.NoError(t, err)
		require.NotNil(t, status)
		assert.GreaterOrEqual(t, status.Total, 0)

		regen := true
		status, err = mr.mutationTriggerEmbedding(ctx, &regen)
		require.NoError(t, err)
		require.NotNil(t, status)

		decay, err := mr.mutationRunDecay(ctx)
		require.NoError(t, err)
		require.NotNil(t, decay)
		assert.Equal(t, 0, decay.NodesProcessed)
	})
}

func TestGraphQLNodeAndSubscriptionAdditionalCoverage(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	nr := &nodeResolver{resolver}

	source := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Source"})
	target := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Target"})
	createEdgeViaCypher(t, resolver, source.ID, target.ID, "KNOWS", nil)

	t.Run("outgoing incoming and similar wrappers", func(t *testing.T) {
		node := &models.Node{ID: source.ID}
		outgoing, err := nr.nodeOutgoing(ctx, node, nil, nil)
		require.NoError(t, err)
		assert.Len(t, outgoing, 1)

		incomingNode := &models.Node{ID: target.ID}
		incoming, err := nr.nodeIncoming(ctx, incomingNode, nil, nil)
		require.NoError(t, err)
		assert.Len(t, incoming, 1)

		similar, err := nr.nodeSimilar(ctx, node, nil, nil)
		require.ErrorContains(t, err, "no embedding")
		assert.Nil(t, similar)
	})

	t.Run("subscription helpers validate broker and delegate", func(t *testing.T) {
		empty := &subscriptionResolver{&Resolver{}}

		_, err := empty.subscriptionNodeCreated(ctx, nil)
		require.ErrorContains(t, err, "event broker not initialized")
		_, err = empty.subscriptionNodeUpdated(ctx, nil, nil)
		require.ErrorContains(t, err, "event broker not initialized")
		_, err = empty.subscriptionNodeDeleted(ctx, nil)
		require.ErrorContains(t, err, "event broker not initialized")
		_, err = empty.subscriptionRelationshipCreated(ctx, nil)
		require.ErrorContains(t, err, "event broker not initialized")
		_, err = empty.subscriptionRelationshipUpdated(ctx, nil, nil)
		require.ErrorContains(t, err, "event broker not initialized")
		_, err = empty.subscriptionRelationshipDeleted(ctx, nil)
		require.ErrorContains(t, err, "event broker not initialized")

		real := &subscriptionResolver{resolver}
		nodeCreated, err := real.subscriptionNodeCreated(ctx, []string{"Person"})
		require.NoError(t, err)
		require.NotNil(t, nodeCreated)

		nodeUpdated, err := real.subscriptionNodeUpdated(ctx, nil, []string{"Person"})
		require.NoError(t, err)
		require.NotNil(t, nodeUpdated)

		nodeDeleted, err := real.subscriptionNodeDeleted(ctx, []string{"Person"})
		require.NoError(t, err)
		require.NotNil(t, nodeDeleted)

		relCreated, err := real.subscriptionRelationshipCreated(ctx, []string{"KNOWS"})
		require.NoError(t, err)
		require.NotNil(t, relCreated)

		relUpdated, err := real.subscriptionRelationshipUpdated(ctx, nil, []string{"KNOWS"})
		require.NoError(t, err)
		require.NotNil(t, relUpdated)

		relDeleted, err := real.subscriptionRelationshipDeleted(ctx, []string{"KNOWS"})
		require.NoError(t, err)
		require.NotNil(t, relDeleted)

		_, err = real.subscriptionSearchStream(ctx, "alice", nil)
		require.ErrorContains(t, err, "not yet implemented")
	})
}

func TestStorageModelConversionHelpers(t *testing.T) {
	t.Run("storage node conversion handles nil and values", func(t *testing.T) {
		assert.Nil(t, storageNodeToModel(nil))

		now := time.Now()
		node := &storage.Node{
			ID:        "storage-node",
			Labels:    []string{"Doc"},
			CreatedAt: now,
			Properties: map[string]interface{}{
				"title": "hello",
			},
		}
		model := storageNodeToModel(node)
		require.NotNil(t, model)
		assert.Equal(t, "storage-node", model.ID)
		assert.Equal(t, "hello", model.Properties["title"])
	})

	t.Run("storage edge conversion handles nil and values", func(t *testing.T) {
		assert.Nil(t, storageEdgeToModel(nil))

		now := time.Now()
		edge := &storage.Edge{
			ID:        "storage-edge",
			StartNode: "a",
			EndNode:   "b",
			Type:      "LINKS",
			CreatedAt: now,
			Properties: map[string]interface{}{
				"weight": 1,
			},
		}
		model := storageEdgeToModel(edge)
		require.NotNil(t, model)
		assert.Equal(t, "storage-edge", model.ID)
		assert.Equal(t, "LINKS", model.Type)
		assert.EqualValues(t, 1, model.Properties["weight"])
	})
}

func TestQuerySimilarAdditionalCoverage(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	qr := &queryResolver{resolver}

	mem1Node := &storage.Node{
		ID:              storage.NodeID(uuid.New().String()),
		Labels:          []string{"Memory"},
		Properties:      map[string]interface{}{"content": "dog"},
		ChunkEmbeddings: [][]float32{{1.0, 0.0, 0.0}},
	}
	mem1ID, err := db.GetStorage().CreateNode(mem1Node)
	require.NoError(t, err)
	puppyNode := &storage.Node{
		ID:              storage.NodeID(uuid.New().String()),
		Labels:          []string{"Memory"},
		Properties:      map[string]interface{}{"content": "puppy"},
		ChunkEmbeddings: [][]float32{{0.95, 0.05, 0.0}},
	}
	_, err = db.GetStorage().CreateNode(puppyNode)
	require.NoError(t, err)
	catNode := &storage.Node{
		ID:              storage.NodeID(uuid.New().String()),
		Labels:          []string{"Memory"},
		Properties:      map[string]interface{}{"content": "cat"},
		ChunkEmbeddings: [][]float32{{0.0, 1.0, 0.0}},
	}
	_, err = db.GetStorage().CreateNode(catNode)
	require.NoError(t, err)

	limit := 2
	results, err := qr.querySimilar(ctx, string(mem1ID), &limit, nil)
	require.NoError(t, err)
	require.NotEmpty(t, results)
	for _, result := range results {
		assert.NotNil(t, result.Node)
		assert.NotEmpty(t, result.Node.ID)
		assert.GreaterOrEqual(t, result.Similarity, -1.0)
		assert.LessOrEqual(t, result.Similarity, 1.0)
	}

	nr := &nodeResolver{resolver}
	nodeResults, err := nr.nodeSimilar(ctx, &models.Node{ID: string(mem1ID)}, &limit, nil)
	require.NoError(t, err)
	require.NotEmpty(t, nodeResults)
}

func TestNamespacedCypherHelperAdditionalCoverage(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)

	t.Run("create edge reports missing node diagnostics", func(t *testing.T) {
		source := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Source"})

		_, err := resolver.createEdgeViaCypher(ctx, source.ID, "missing-target", "KNOWS", nil)
		require.Error(t, err)
		assert.NotEmpty(t, err.Error())
	})

	t.Run("direct edge helpers cover lookup and listing paths", func(t *testing.T) {
		source := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Source2"})
		target := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Target2"})
		incoming := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Incoming2"})

		outEdge := createEdgeViaCypher(t, resolver, source.ID, target.ID, "KNOWS", map[string]interface{}{"since": "2024"})
		createEdgeViaCypher(t, resolver, incoming.ID, source.ID, "REPORTS_TO", nil)

		got, err := resolver.getEdgeViaCypher(ctx, outEdge.ID)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, outEdge.ID, got.ID)
		assert.Equal(t, "KNOWS", got.Type)

		edges, err := resolver.getEdgesForNodeViaCypher(ctx, source.ID)
		require.NoError(t, err)
		assert.Len(t, edges, 2)

		listed, err := resolver.listEdgesViaCypher(ctx, "KNOWS", 10, 0)
		require.NoError(t, err)
		require.NotEmpty(t, listed)
		assert.Equal(t, "KNOWS", listed[0].Type)

		_, err = resolver.getEdgeViaCypher(ctx, "missing-edge")
		require.ErrorContains(t, err, "edge not found")
	})

	t.Run("extract node handles element id top-level properties and errors", func(t *testing.T) {
		node, err := extractNodeFromResult(map[string]interface{}{
			"elementId": "4:nornicdb:node-123",
			"labels":    []interface{}{"Person", "Employee"},
			"name":      "Alice",
			"age":       30,
		})
		require.NoError(t, err)
		require.NotNil(t, node)
		assert.Equal(t, "node-123", node.ID)
		assert.Contains(t, node.Labels, "Person")
		assert.Equal(t, "Alice", node.Properties["name"])
		assert.EqualValues(t, 30, node.Properties["age"])

		_, err = extractNodeFromResult(struct{}{})
		require.ErrorContains(t, err, "unexpected node format")

		_, err = extractNodeFromResult(map[string]interface{}{"labels": []string{"NoID"}})
		require.ErrorContains(t, err, "node missing ID field")
	})

	t.Run("extract edge handles fallback sources element ids and errors", func(t *testing.T) {
		edge, err := extractEdgeFromResult([]interface{}{
			&storage.Edge{
				ID:         "edge-1",
				StartNode:  "start-a",
				EndNode:    "end-b",
				Type:       "KNOWS",
				Properties: map[string]interface{}{"since": "2020"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "start-a", edge.Source)
		assert.Equal(t, "end-b", edge.Target)

		edge, err = extractEdgeFromResult([]interface{}{
			map[string]interface{}{
				"elementId": "5:nornicdb:edge-2",
				"type":      "WORKS_WITH",
				"startNode": "left",
				"endNode":   "right",
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "edge-2", edge.ID)
		assert.Equal(t, "left", edge.Source)
		assert.Equal(t, "right", edge.Target)
		assert.Empty(t, edge.Properties)

		edge, err = extractEdgeFromResult([]interface{}{
			map[string]interface{}{
				"_edgeId":    "edge-3",
				"type":       "REPORTS_TO",
				"properties": map[string]interface{}{"active": true},
			},
			&storage.Node{ID: "manager"},
			map[string]interface{}{"id": "employee"},
		})
		require.NoError(t, err)
		assert.Equal(t, "manager", edge.Source)
		assert.Equal(t, "employee", edge.Target)
		assert.Equal(t, "REPORTS_TO", edge.Type)
		assert.Equal(t, true, edge.Properties["active"])

		edge, err = extractEdgeFromResult([]interface{}{
			&storage.Edge{
				ID:         "edge-4",
				StartNode:  "fallback-source",
				EndNode:    "fallback-target",
				Type:       "MENTORS",
				Properties: map[string]interface{}{"strength": int64(9)},
			},
			map[string]interface{}{"_nodeId": "explicit-source"},
			"explicit-target",
		})
		require.NoError(t, err)
		assert.Equal(t, "explicit-source", edge.Source)
		assert.Equal(t, "explicit-target", edge.Target)
		assert.Equal(t, "MENTORS", edge.Type)
		assert.EqualValues(t, 9, edge.Properties["strength"])

		edge, err = extractEdgeFromResult([]interface{}{
			map[string]interface{}{
				"id":         "edge-5",
				"type":       "FOLLOWS",
				"properties": map[string]interface{}{"since": "2024"},
			},
			"source-as-string",
			&storage.Node{ID: "target-as-node"},
		})
		require.NoError(t, err)
		assert.Equal(t, "edge-5", edge.ID)
		assert.Equal(t, "source-as-string", edge.Source)
		assert.Equal(t, "target-as-node", edge.Target)
		assert.Equal(t, "FOLLOWS", edge.Type)
		assert.Equal(t, "2024", edge.Properties["since"])

		_, err = extractEdgeFromResult([]interface{}{})
		require.ErrorContains(t, err, "insufficient edge data")

		_, err = extractEdgeFromResult([]interface{}{42})
		require.ErrorContains(t, err, "unexpected edge format")

		_, err = extractEdgeFromResult([]interface{}{map[string]interface{}{"type": "BROKEN"}})
		require.ErrorContains(t, err, "edge missing ID field")
	})
}

func TestResolverAccessControlAdditionalCoverage(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)

	t.Run("cypher executor enforces database access mode", func(t *testing.T) {
		deniedCtx := auth.WithRequestDatabaseAccessMode(ctx, auth.DenyAllDatabaseAccessMode)
		_, err := resolver.getCypherExecutor(deniedCtx, "nornic")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not allowed")
	})

	t.Run("cypher executor errors for missing database", func(t *testing.T) {
		_, err := resolver.getCypherExecutor(ctx, "missing-db")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("execute cypher enforces write access for mutations", func(t *testing.T) {
		deniedCtx := auth.WithRequestResolvedAccessResolver(ctx, func(string) auth.ResolvedAccess {
			return auth.ResolvedAccess{Read: true, Write: false}
		})
		_, err := resolver.executeCypher(deniedCtx, "CREATE (n:Denied)", nil, "", true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "write on database")
	})

	t.Run("execute cypher allows write when resolver grants access", func(t *testing.T) {
		allowedCtx := auth.WithRequestResolvedAccessResolver(ctx, func(string) auth.ResolvedAccess {
			return auth.ResolvedAccess{Read: true, Write: true}
		})
		result, err := resolver.executeCypher(allowedCtx, "CREATE (n:Allowed {name: 'ok'}) RETURN n", nil, "", true)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.NotEmpty(t, result.Rows)
	})
}

func TestQuerySearchAndStatsAdditionalCoverage(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	qr := &queryResolver{resolver}

	aliceNode := &storage.Node{
		ID:              storage.NodeID(uuid.New().String()),
		Labels:          []string{"Memory"},
		Properties:      map[string]interface{}{"content": "Alice builds graph search systems", "title": "Alice Memory"},
		ChunkEmbeddings: [][]float32{{1.0, 0.0, 0.0}},
	}
	storedID, err := db.GetStorage().CreateNode(aliceNode)
	require.NoError(t, err)
	require.NotEmpty(t, storedID)

	bobNode := &storage.Node{
		ID:              storage.NodeID(uuid.New().String()),
		Labels:          []string{"Memory"},
		Properties:      map[string]interface{}{"content": "Bob maintains retrieval pipelines", "title": "Bob Memory"},
		ChunkEmbeddings: [][]float32{{0.9, 0.1, 0.0}},
	}
	_, err = db.GetStorage().CreateNode(bobNode)
	require.NoError(t, err)

	require.NoError(t, db.BuildSearchIndexes(ctx))

	t.Run("query search exposes metadata for matches", func(t *testing.T) {
		limit := 5
		resp, err := qr.querySearch(ctx, "Alice graph", &models.SearchOptions{Limit: &limit})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotEmpty(t, resp.Results)
		assert.GreaterOrEqual(t, resp.TotalCount, 1)
		assert.GreaterOrEqual(t, resp.ExecutionTimeMs, float64(0))
		assert.NotNil(t, resp.Results[0].Node)
		assert.NotEmpty(t, resp.Results[0].FoundBy)
	})

	t.Run("query stats reports labels relationships and embedded node count", func(t *testing.T) {
		source := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Source"})
		target := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Target"})
		createEdgeViaCypher(t, resolver, source.ID, target.ID, "KNOWS", nil)

		stats, err := qr.queryStats(ctx)
		require.NoError(t, err)
		require.NotNil(t, stats)
		assert.GreaterOrEqual(t, stats.NodeCount, 2)
		assert.GreaterOrEqual(t, stats.RelationshipCount, 1)
		assert.GreaterOrEqual(t, stats.EmbeddedNodeCount, 1)
		assert.Greater(t, stats.UptimeSeconds, float64(0))

		var sawMemory, sawKnows bool
		for _, label := range stats.Labels {
			if label.Label == "Memory" {
				sawMemory = true
			}
		}
		for _, relType := range stats.RelationshipTypes {
			if relType.Type == "KNOWS" {
				sawKnows = true
			}
		}
		assert.True(t, sawMemory)
		assert.True(t, sawKnows)
	})
}

func TestResolverNamespacedErrorAndFallbackCoverage(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	qr := &queryResolver{resolver}

	source := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Alice", "role": "Engineer"})
	target := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{"name": "Bob", "role": "Engineer"})
	createEdgeViaCypher(t, resolver, source.ID, target.ID, "KNOWS", map[string]interface{}{"since": "2024"})

	t.Run("label helper returns labels via CALL path", func(t *testing.T) {
		labels, err := resolver.getLabelsViaCypher(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, labels)
		assert.Contains(t, labels, "Person")
	})

	t.Run("label helper fallback returns final access error", func(t *testing.T) {
		deniedCtx := auth.WithRequestDatabaseAccessMode(ctx, auth.DenyAllDatabaseAccessMode)
		_, err := resolver.getLabelsViaCypher(deniedCtx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not allowed")
	})

	t.Run("edge helpers surface access-control errors", func(t *testing.T) {
		deniedCtx := auth.WithRequestDatabaseAccessMode(ctx, auth.DenyAllDatabaseAccessMode)

		_, err := resolver.getEdgeViaCypher(deniedCtx, "edge-id")
		require.Error(t, err)

		_, err = resolver.getEdgesForNodeViaCypher(deniedCtx, source.ID)
		require.Error(t, err)

		_, err = resolver.listEdgesViaCypher(deniedCtx, "", 10, 0)
		require.Error(t, err)
	})

	t.Run("createEdgeViaCypher returns deterministic error on match miss", func(t *testing.T) {
		_, err := resolver.createEdgeViaCypher(ctx, source.ID, "missing-target", "KNOWS", map[string]interface{}{"since": "2024"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create relationship: source node")
		assert.Contains(t, err.Error(), "exists=true")
		assert.Contains(t, err.Error(), "target=missing-target")
	})

	t.Run("query search by property covers labels+limit and parse branches", func(t *testing.T) {
		metaNode := createNodeViaCypher(t, resolver, []string{"Person"}, map[string]interface{}{
			"name": "MetaCarrier",
			"meta": map[string]interface{}{"value": "Alice"},
		})
		require.NotNil(t, metaNode)

		limit := 1
		nodes, err := qr.querySearchByProperty(ctx, "meta", models.JSON{"value": "Alice"}, []string{"Person"}, &limit)
		require.NoError(t, err)
		assert.NotNil(t, nodes)
		if len(nodes) > 0 {
			assert.Equal(t, "MetaCarrier", nodes[0].Properties["name"])
		}

		none, err := qr.querySearchByProperty(ctx, "missing_key", models.JSON{"value": "x"}, nil, nil)
		require.NoError(t, err)
		assert.Empty(t, none)

		deniedCtx := auth.WithRequestDatabaseAccessMode(ctx, auth.DenyAllDatabaseAccessMode)
		_, err = qr.querySearchByProperty(deniedCtx, "meta", models.JSON{"value": "Alice"}, nil, nil)
		require.Error(t, err)
	})
}

func TestMutationAndRelationshipErrorCoverage(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	mr := &mutationResolver{resolver}
	rr := &relationshipResolver{resolver}

	t.Run("mutation wrappers return surfaced errors", func(t *testing.T) {
		_, err := mr.mutationCreateNode(ctx, models.CreateNodeInput{Labels: []string{"Person"}, Properties: nil})
		require.NoError(t, err)

		_, err = mr.mutationUpdateNode(ctx, models.UpdateNodeInput{
			ID:         "missing-node",
			Properties: models.JSON{"name": "x"},
		})
		require.Error(t, err)

		ok, err := mr.mutationDeleteNode(ctx, "missing-node")
		require.Error(t, err)
		require.False(t, ok)

		deniedCtx := auth.WithRequestDatabaseAccessMode(ctx, auth.DenyAllDatabaseAccessMode)
		ok, err = mr.mutationDeleteRelationship(deniedCtx, "missing-edge")
		require.Error(t, err)
		require.False(t, ok)

		ok, err = mr.mutationRebuildSearchIndex(ctx)
		require.NoError(t, err)
		require.True(t, ok)

		ok, err = mr.mutationClearAll(ctx, "wrong phrase")
		require.Error(t, err)
		require.False(t, ok)
	})

	t.Run("relationship resolvers return errors for missing endpoint nodes", func(t *testing.T) {
		_, err := rr.relationshipStartNode(ctx, &models.Relationship{
			StartNodeID: "missing-start",
			EndNodeID:   "missing-end",
		})
		require.Error(t, err)

		_, err = rr.relationshipEndNode(ctx, &models.Relationship{
			StartNodeID: "missing-start",
			EndNodeID:   "missing-end",
		})
		require.Error(t, err)
	})
}

func TestCoerceResultNodeID(t *testing.T) {
	assert.Equal(t, "node-1", coerceResultNodeID("node-1"))
	assert.Equal(t, "node-2", coerceResultNodeID(map[string]interface{}{"id": "node-2"}))
	assert.Equal(t, "node-3", coerceResultNodeID(map[string]interface{}{"_nodeId": "node-3"}))
	assert.Equal(t, "node-4", coerceResultNodeID(&storage.Node{ID: "node-4"}))
	assert.Equal(t, "", coerceResultNodeID(map[string]interface{}{"id": 123}))
	assert.Equal(t, "", coerceResultNodeID(42))
}

// ---------------------------------------------------------------------------
// database_manager_adapter – delegate methods
// ---------------------------------------------------------------------------

func TestGraphqlDatabaseManagerAdapter(t *testing.T) {
	db := testDB(t)
	manager := testDBManager(t, db)
	adapter := &graphqlDatabaseManagerAdapter{manager: manager}

	t.Run("Exists", func(t *testing.T) {
		assert.True(t, adapter.Exists("nornic"))
		assert.False(t, adapter.Exists("nonexistent"))
	})

	t.Run("CreateDatabase and DropDatabase", func(t *testing.T) {
		err := adapter.CreateDatabase("adapter-test-db")
		require.NoError(t, err)
		assert.True(t, adapter.Exists("adapter-test-db"))

		err = adapter.DropDatabase("adapter-test-db")
		require.NoError(t, err)
		assert.False(t, adapter.Exists("adapter-test-db"))
	})

	t.Run("CreateAlias DropAlias ListAliases ResolveDatabase", func(t *testing.T) {
		err := adapter.CreateAlias("myalias", "nornic")
		require.NoError(t, err)

		aliases := adapter.ListAliases("nornic")
		assert.Contains(t, aliases, "myalias")

		resolved, err := adapter.ResolveDatabase("myalias")
		require.NoError(t, err)
		assert.Equal(t, "nornic", resolved)

		err = adapter.DropAlias("myalias")
		require.NoError(t, err)
	})

	t.Run("ListDatabases", func(t *testing.T) {
		dbs := adapter.ListDatabases()
		assert.NotEmpty(t, dbs)
		found := false
		for _, d := range dbs {
			if d.Name() == "nornic" {
				found = true
				assert.Equal(t, "standard", d.Type())
				assert.Equal(t, "online", d.Status())
				assert.True(t, d.IsDefault())
				// CreatedAt may be zero for in-memory, just call it
				_ = d.CreatedAt()
			}
		}
		assert.True(t, found, "nornic database should be in list")
	})

	t.Run("ListCompositeDatabases", func(t *testing.T) {
		composites := adapter.ListCompositeDatabases()
		// No composites created, should be empty
		assert.Empty(t, composites)
	})

	t.Run("IsCompositeDatabase", func(t *testing.T) {
		assert.False(t, adapter.IsCompositeDatabase("nornic"))
	})

	t.Run("SetDatabaseLimits and GetDatabaseLimits", func(t *testing.T) {
		limits := &multidb.Limits{
			Storage: multidb.StorageLimits{MaxNodes: 1000},
		}
		err := adapter.SetDatabaseLimits("nornic", limits)
		require.NoError(t, err)

		got, err := adapter.GetDatabaseLimits("nornic")
		require.NoError(t, err)
		assert.NotNil(t, got)
	})

	t.Run("SetDatabaseLimits invalid type", func(t *testing.T) {
		err := adapter.SetDatabaseLimits("nornic", "not-a-limits-struct")
		assert.Error(t, err)
	})

	t.Run("GetStorageForUse", func(t *testing.T) {
		store, err := adapter.GetStorageForUse("nornic", "")
		require.NoError(t, err)
		assert.NotNil(t, store)
	})

	t.Run("GetCompositeConstituents non-composite", func(t *testing.T) {
		_, err := adapter.GetCompositeConstituents("nornic")
		assert.Error(t, err)
	})

	t.Run("CreateCompositeDatabase with map constituents", func(t *testing.T) {
		constituents := []interface{}{
			map[string]interface{}{
				"alias":         "local",
				"database_name": "nornic",
				"type":          "local",
				"access_mode":   "read_write",
			},
		}
		err := adapter.CreateCompositeDatabase("comp1", constituents)
		require.NoError(t, err)

		cons, err := adapter.GetCompositeConstituents("comp1")
		require.NoError(t, err)
		assert.Len(t, cons, 1)

		err = adapter.DropCompositeDatabase("comp1")
		require.NoError(t, err)
	})

	t.Run("CreateCompositeDatabase with ConstituentRef", func(t *testing.T) {
		constituents := []interface{}{
			multidb.ConstituentRef{
				Alias:        "local",
				DatabaseName: "nornic",
				Type:         "local",
				AccessMode:   "read_write",
			},
		}
		err := adapter.CreateCompositeDatabase("comp2", constituents)
		require.NoError(t, err)
		err = adapter.DropCompositeDatabase("comp2")
		require.NoError(t, err)
	})

	t.Run("CreateCompositeDatabase invalid constituent type", func(t *testing.T) {
		err := adapter.CreateCompositeDatabase("comp3", []interface{}{42})
		assert.Error(t, err)
	})

	t.Run("AddConstituent with map", func(t *testing.T) {
		err := adapter.CreateCompositeDatabase("comp4", []interface{}{
			multidb.ConstituentRef{Alias: "first", DatabaseName: "nornic", Type: "local", AccessMode: "read_write"},
		})
		require.NoError(t, err)

		err = adapter.AddConstituent("comp4", map[string]interface{}{
			"alias":         "second",
			"database_name": "nornic",
			"type":          "local",
			"access_mode":   "read",
		})
		require.NoError(t, err)
		_ = adapter.DropCompositeDatabase("comp4")
	})

	t.Run("AddConstituent with ConstituentRef", func(t *testing.T) {
		err := adapter.CreateCompositeDatabase("comp5", []interface{}{
			multidb.ConstituentRef{Alias: "first", DatabaseName: "nornic", Type: "local", AccessMode: "read_write"},
		})
		require.NoError(t, err)

		err = adapter.AddConstituent("comp5", multidb.ConstituentRef{
			Alias: "second", DatabaseName: "nornic", Type: "local", AccessMode: "read",
		})
		require.NoError(t, err)
		_ = adapter.DropCompositeDatabase("comp5")
	})

	t.Run("AddConstituent invalid type", func(t *testing.T) {
		err := adapter.AddConstituent("nornic", 42)
		assert.Error(t, err)
	})

	t.Run("RemoveConstituent", func(t *testing.T) {
		err := adapter.CreateCompositeDatabase("comp6", []interface{}{
			multidb.ConstituentRef{Alias: "first", DatabaseName: "nornic", Type: "local", AccessMode: "read_write"},
			multidb.ConstituentRef{Alias: "second", DatabaseName: "nornic", Type: "local", AccessMode: "read"},
		})
		require.NoError(t, err)

		err = adapter.RemoveConstituent("comp6", "second")
		require.NoError(t, err)
		_ = adapter.DropCompositeDatabase("comp6")
	})
}

func TestAdapterString(t *testing.T) {
	m := map[string]interface{}{"key": "val", "num": 42}
	assert.Equal(t, "val", adapterString(m, "key"))
	assert.Equal(t, "", adapterString(m, "num"))  // not a string
	assert.Equal(t, "", adapterString(m, "miss")) // missing key
}

// ---------------------------------------------------------------------------
// helpers.go – storageNodeToModel / storageEdgeToModel edge cases
// ---------------------------------------------------------------------------

func TestStorageNodeToModel_Nil(t *testing.T) {
	assert.Nil(t, storageNodeToModel(nil))
}

func TestStorageNodeToModel_WithTimestamps(t *testing.T) {
	now := time.Now()
	node := &storage.Node{
		ID:              "n1",
		Labels:          []string{"Person"},
		Properties:      map[string]interface{}{"name": "Alice"},
		CreatedAt:       now,
		UpdatedAt:       now,
		ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
	}
	m := storageNodeToModel(node)
	require.NotNil(t, m)
	assert.Equal(t, "n1", m.ID)
	assert.NotNil(t, m.CreatedAt)
	assert.NotNil(t, m.UpdatedAt)
	assert.True(t, m.HasEmbedding)
	assert.Equal(t, 3, m.EmbeddingDimensions)
}

func TestStorageNodeToModel_NoTimestamps(t *testing.T) {
	node := &storage.Node{
		ID:     "n2",
		Labels: []string{"Thing"},
	}
	m := storageNodeToModel(node)
	assert.Nil(t, m.CreatedAt)
	assert.Nil(t, m.UpdatedAt)
	assert.Nil(t, m.LastAccessed)
	assert.False(t, m.HasEmbedding)
	assert.Equal(t, 0, m.EmbeddingDimensions)
}

func TestStorageEdgeToModel_Nil(t *testing.T) {
	assert.Nil(t, storageEdgeToModel(nil))
}

func TestStorageEdgeToModel_WithTimestamps(t *testing.T) {
	now := time.Now()
	edge := &storage.Edge{
		ID:            "e1",
		StartNode:     "n1",
		EndNode:       "n2",
		Type:          "KNOWS",
		Properties:    map[string]interface{}{"since": 2020},
		CreatedAt:     now,
		UpdatedAt:     now,
		Confidence:    0.85,
		AutoGenerated: true,
	}
	m := storageEdgeToModel(edge)
	require.NotNil(t, m)
	assert.Equal(t, "e1", m.ID)
	assert.Equal(t, "n1", m.StartNodeID)
	assert.Equal(t, "n2", m.EndNodeID)
	assert.Equal(t, "KNOWS", m.Type)
	assert.NotNil(t, m.CreatedAt)
	assert.NotNil(t, m.UpdatedAt)
	assert.InDelta(t, 0.85, *m.Confidence, 0.01)
	assert.True(t, m.AutoGenerated)
}

func TestStorageEdgeToModel_NoTimestamps(t *testing.T) {
	edge := &storage.Edge{
		ID:        "e2",
		StartNode: "n1",
		EndNode:   "n2",
		Type:      "LIKES",
	}
	m := storageEdgeToModel(edge)
	assert.Nil(t, m.CreatedAt)
	assert.Nil(t, m.UpdatedAt)
}

// ---------------------------------------------------------------------------
// helpers_namespaced.go – buildLabelsString
// ---------------------------------------------------------------------------

func TestBuildLabelsString(t *testing.T) {
	assert.Equal(t, "", buildLabelsString(nil))
	assert.Equal(t, "", buildLabelsString([]string{}))
	assert.Equal(t, ":Person", buildLabelsString([]string{"Person"}))
	assert.Equal(t, ":Person:Employee", buildLabelsString([]string{"Person", "Employee"}))
}

// ---------------------------------------------------------------------------
// helpers_namespaced.go – extractNodeFromResult
// ---------------------------------------------------------------------------

func TestExtractNodeFromResult_MapWithNodeId(t *testing.T) {
	val := map[string]interface{}{
		"_nodeId":    "n-123",
		"labels":     []interface{}{"Person"},
		"properties": map[string]interface{}{"name": "Alice"},
	}
	node, err := extractNodeFromResult(val)
	require.NoError(t, err)
	assert.Equal(t, "n-123", node.ID)
	assert.Equal(t, []string{"Person"}, node.Labels)
	assert.Equal(t, "Alice", node.Properties["name"])
}

func TestExtractNodeFromResult_MapWithElementId(t *testing.T) {
	// Neo4j 5 elementId format: "4:nornicdb:uuid-here"
	val := map[string]interface{}{
		"elementId": "4:nornicdb:actual-uuid",
		"labels":    []string{"Company"},
	}
	node, err := extractNodeFromResult(val)
	require.NoError(t, err)
	assert.Equal(t, "actual-uuid", node.ID)
}

func TestExtractNodeFromResult_MapWithTopLevelProps(t *testing.T) {
	// When no "properties" key exists, top-level keys become properties
	val := map[string]interface{}{
		"id":   "n-456",
		"name": "Bob",
		"age":  30,
	}
	node, err := extractNodeFromResult(val)
	require.NoError(t, err)
	assert.Equal(t, "Bob", node.Properties["name"])
	assert.Equal(t, 30, node.Properties["age"])
	// "id" should not be in properties
	_, hasID := node.Properties["id"]
	assert.False(t, hasID)
}

func TestExtractNodeFromResult_StorageNode(t *testing.T) {
	val := &storage.Node{
		ID:         "sn-1",
		Labels:     []string{"Device"},
		Properties: map[string]interface{}{"serial": "ABC"},
	}
	node, err := extractNodeFromResult(val)
	require.NoError(t, err)
	assert.Equal(t, "sn-1", node.ID)
	assert.Equal(t, []string{"Device"}, node.Labels)
}

func TestExtractNodeFromResult_MissingID(t *testing.T) {
	val := map[string]interface{}{"labels": []string{"Ghost"}}
	_, err := extractNodeFromResult(val)
	assert.Error(t, err)
}

func TestExtractNodeFromResult_UnsupportedType(t *testing.T) {
	_, err := extractNodeFromResult(42)
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// helpers_namespaced.go – extractEdgeFromResult
// ---------------------------------------------------------------------------

func TestExtractEdgeFromResult_StorageEdge(t *testing.T) {
	edge := &storage.Edge{
		ID:         "e-1",
		Type:       "KNOWS",
		StartNode:  "n-a",
		EndNode:    "n-b",
		Properties: map[string]interface{}{"since": 2020},
	}
	// Row with 3 elements: edge, source ID, target ID
	row := []interface{}{edge, "n-a", "n-b"}
	result, err := extractEdgeFromResult(row)
	require.NoError(t, err)
	assert.Equal(t, "e-1", result.ID)
	assert.Equal(t, "KNOWS", result.Type)
	assert.Equal(t, "n-a", result.Source)
	assert.Equal(t, "n-b", result.Target)
	assert.Equal(t, 2020, result.Properties["since"])
}

func TestExtractEdgeFromResult_StorageEdgeFallbackEndpoints(t *testing.T) {
	// When row has only the edge (no source/target), fallback to StartNode/EndNode
	edge := &storage.Edge{
		ID:        "e-2",
		Type:      "LIKES",
		StartNode: "n-x",
		EndNode:   "n-y",
	}
	row := []interface{}{edge}
	result, err := extractEdgeFromResult(row)
	require.NoError(t, err)
	assert.Equal(t, "n-x", result.Source)
	assert.Equal(t, "n-y", result.Target)
}

func TestExtractEdgeFromResult_StorageEdgeWithNodeMaps(t *testing.T) {
	edge := &storage.Edge{ID: "e-3", Type: "WORKS_AT", StartNode: "n-1", EndNode: "n-2"}
	row := []interface{}{
		edge,
		map[string]interface{}{"_nodeId": "n-1"},
		map[string]interface{}{"id": "n-2"},
	}
	result, err := extractEdgeFromResult(row)
	require.NoError(t, err)
	assert.Equal(t, "n-1", result.Source)
	assert.Equal(t, "n-2", result.Target)
}

func TestExtractEdgeFromResult_StorageEdgeWithStorageNodes(t *testing.T) {
	edge := &storage.Edge{ID: "e-4", Type: "KNOWS", StartNode: "n-1", EndNode: "n-2"}
	row := []interface{}{
		edge,
		&storage.Node{ID: "n-1"},
		&storage.Node{ID: "n-2"},
	}
	result, err := extractEdgeFromResult(row)
	require.NoError(t, err)
	assert.Equal(t, "n-1", result.Source)
	assert.Equal(t, "n-2", result.Target)
}

func TestExtractEdgeFromResult_MapFormat(t *testing.T) {
	edgeMap := map[string]interface{}{
		"_edgeId":    "e-5",
		"type":       "KNOWS",
		"properties": map[string]interface{}{"weight": 0.9},
	}
	row := []interface{}{edgeMap, "src-1", "tgt-1"}
	result, err := extractEdgeFromResult(row)
	require.NoError(t, err)
	assert.Equal(t, "e-5", result.ID)
	assert.Equal(t, "KNOWS", result.Type)
	assert.Equal(t, "src-1", result.Source)
	assert.Equal(t, "tgt-1", result.Target)
	assert.Equal(t, 0.9, result.Properties["weight"])
}

func TestExtractEdgeFromResult_MapWithElementId(t *testing.T) {
	edgeMap := map[string]interface{}{
		"elementId": "5:nornicdb:edge-uuid",
		"type":      "REVIEWED",
	}
	row := []interface{}{edgeMap}
	result, err := extractEdgeFromResult(row)
	require.NoError(t, err)
	assert.Equal(t, "edge-uuid", result.ID)
}

func TestExtractEdgeFromResult_MapFallbackEndpoints(t *testing.T) {
	edgeMap := map[string]interface{}{
		"id":        "e-6",
		"type":      "CONTAINS",
		"startNode": "n-start",
		"endNode":   "n-end",
	}
	row := []interface{}{edgeMap}
	result, err := extractEdgeFromResult(row)
	require.NoError(t, err)
	assert.Equal(t, "n-start", result.Source)
	assert.Equal(t, "n-end", result.Target)
}

func TestExtractEdgeFromResult_MapWithNodeMapEndpoints(t *testing.T) {
	edgeMap := map[string]interface{}{"id": "e-7", "type": "OWNS"}
	row := []interface{}{
		edgeMap,
		map[string]interface{}{"_nodeId": "owner"},
		map[string]interface{}{"id": "asset"},
	}
	result, err := extractEdgeFromResult(row)
	require.NoError(t, err)
	assert.Equal(t, "owner", result.Source)
	assert.Equal(t, "asset", result.Target)
}

func TestExtractEdgeFromResult_MapWithStorageNodeEndpoints(t *testing.T) {
	edgeMap := map[string]interface{}{"id": "e-8", "type": "MANAGES"}
	row := []interface{}{
		edgeMap,
		&storage.Node{ID: "mgr"},
		&storage.Node{ID: "report"},
	}
	result, err := extractEdgeFromResult(row)
	require.NoError(t, err)
	assert.Equal(t, "mgr", result.Source)
	assert.Equal(t, "report", result.Target)
}

func TestExtractEdgeFromResult_MapMissingID(t *testing.T) {
	edgeMap := map[string]interface{}{"type": "UNKNOWN"}
	row := []interface{}{edgeMap}
	_, err := extractEdgeFromResult(row)
	assert.Error(t, err)
}

func TestExtractEdgeFromResult_MapNoProperties(t *testing.T) {
	edgeMap := map[string]interface{}{"id": "e-9", "type": "PLAIN"}
	row := []interface{}{edgeMap}
	result, err := extractEdgeFromResult(row)
	require.NoError(t, err)
	assert.NotNil(t, result.Properties)
	assert.Empty(t, result.Properties)
}

func TestExtractEdgeFromResult_EmptyRow(t *testing.T) {
	_, err := extractEdgeFromResult([]interface{}{})
	assert.Error(t, err)
}

func TestExtractEdgeFromResult_UnsupportedType(t *testing.T) {
	_, err := extractEdgeFromResult([]interface{}{42})
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// helpers_namespaced.go – buildPropertiesString
// ---------------------------------------------------------------------------

func TestBuildPropertiesString(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		str, params := buildPropertiesString(nil)
		assert.Equal(t, "{}", str)
		assert.Empty(t, params)
	})

	t.Run("single property", func(t *testing.T) {
		str, params := buildPropertiesString(map[string]interface{}{"name": "Alice"})
		assert.Contains(t, str, "name: $p0")
		assert.Equal(t, "Alice", params["p0"])
	})
}

func TestEventBrokerCloseAdditionalCoverage(t *testing.T) {
	broker := NewEventBroker()
	nodeCreated := broker.SubscribeNodeCreated(context.Background(), nil)
	nodeUpdated := broker.SubscribeNodeUpdated(context.Background(), nil, nil)
	nodeDeleted := broker.SubscribeNodeDeleted(context.Background(), nil)
	relCreated := broker.SubscribeRelationshipCreated(context.Background(), nil)
	relUpdated := broker.SubscribeRelationshipUpdated(context.Background(), nil, nil)
	relDeleted := broker.SubscribeRelationshipDeleted(context.Background(), nil)

	broker.Close()

	_, ok := <-nodeCreated
	assert.False(t, ok)
	_, ok = <-nodeUpdated
	assert.False(t, ok)
	_, ok = <-nodeDeleted
	assert.False(t, ok)
	_, ok = <-relCreated
	assert.False(t, ok)
	_, ok = <-relUpdated
	assert.False(t, ok)
	_, ok = <-relDeleted
	assert.False(t, ok)
}
