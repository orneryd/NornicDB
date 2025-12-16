package resolvers

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/graphql/models"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testDB creates a temporary NornicDB instance for testing
func testDB(t *testing.T) *nornicdb.DB {
	t.Helper()
	db, err := nornicdb.Open(t.TempDir(), &nornicdb.Config{
		DecayEnabled:     false,
		AutoLinksEnabled: false,
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestNewResolver(t *testing.T) {
	db := testDB(t)
	resolver := NewResolver(db)

	assert.NotNil(t, resolver)
	assert.Equal(t, db, resolver.DB)
	assert.False(t, resolver.StartTime.IsZero())
}

// =============================================================================
// Query Tests
// =============================================================================

func TestQueryNode(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	resolver := NewResolver(db)
	qr := &queryResolver{resolver}

	t.Run("returns nil for non-existent node", func(t *testing.T) {
		node, err := qr.queryNode(ctx, "non-existent-id")
		assert.NoError(t, err)
		assert.Nil(t, node)
	})

	t.Run("returns node by ID", func(t *testing.T) {
		// Create a node first
		created, err := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{
			"name": "Alice",
			"age":  30,
		})
		require.NoError(t, err)

		// Query it
		node, err := qr.queryNode(ctx, created.ID)
		assert.NoError(t, err)
		require.NotNil(t, node)
		assert.Equal(t, created.ID, node.ID)
		assert.Contains(t, node.Labels, "Person")
		assert.Equal(t, "Alice", node.Properties["name"])
	})
}

func TestQueryNodes(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	resolver := NewResolver(db)
	qr := &queryResolver{resolver}

	t.Run("returns empty for non-existent IDs", func(t *testing.T) {
		nodes, err := qr.queryNodes(ctx, []string{"id1", "id2"})
		assert.NoError(t, err)
		assert.Empty(t, nodes)
	})

	t.Run("returns existing nodes", func(t *testing.T) {
		// Create nodes
		n1, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		n2, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Bob"})

		nodes, err := qr.queryNodes(ctx, []string{n1.ID, n2.ID, "non-existent"})
		assert.NoError(t, err)
		assert.Len(t, nodes, 2)
	})
}

func TestQueryAllNodes(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	resolver := NewResolver(db)
	qr := &queryResolver{resolver}

	t.Run("returns empty when no nodes", func(t *testing.T) {
		nodes, err := qr.queryAllNodes(ctx, nil, nil, nil)
		assert.NoError(t, err)
		assert.Empty(t, nodes)
	})

	t.Run("returns all nodes with limit", func(t *testing.T) {
		// Create nodes
		db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Bob"})
		db.CreateNode(ctx, []string{"Company"}, map[string]interface{}{"name": "TechCorp"})

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
	resolver := NewResolver(db)
	qr := &queryResolver{resolver}

	t.Run("returns 0 when empty", func(t *testing.T) {
		count, err := qr.queryNodeCount(ctx, nil)
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("returns total count", func(t *testing.T) {
		db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		db.CreateNode(ctx, []string{"Company"}, map[string]interface{}{"name": "TechCorp"})

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
	resolver := NewResolver(db)
	qr := &queryResolver{resolver}

	t.Run("returns nil for non-existent relationship", func(t *testing.T) {
		rel, err := qr.queryRelationship(ctx, "non-existent")
		assert.NoError(t, err)
		assert.Nil(t, rel)
	})

	t.Run("returns relationship by ID", func(t *testing.T) {
		// Create nodes and relationship
		n1, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		n2, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Bob"})
		edge, err := db.CreateEdge(ctx, n1.ID, n2.ID, "KNOWS", map[string]interface{}{"since": "2020"})
		require.NoError(t, err)

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
	resolver := NewResolver(db)
	qr := &queryResolver{resolver}

	t.Run("returns relationships between nodes", func(t *testing.T) {
		n1, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		n2, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Bob"})
		db.CreateEdge(ctx, n1.ID, n2.ID, "KNOWS", nil)
		db.CreateEdge(ctx, n1.ID, n2.ID, "WORKS_WITH", nil)

		rels, err := qr.queryRelationshipsBetween(ctx, n1.ID, n2.ID)
		assert.NoError(t, err)
		assert.Len(t, rels, 2)
	})
}

func TestQueryStats(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	resolver := NewResolver(db)
	qr := &queryResolver{resolver}

	t.Run("returns database statistics", func(t *testing.T) {
		db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Bob"})

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
	resolver := NewResolver(db)
	qr := &queryResolver{resolver}

	t.Run("returns all labels", func(t *testing.T) {
		db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		db.CreateNode(ctx, []string{"Company"}, map[string]interface{}{"name": "TechCorp"})

		labels, err := qr.queryLabels(ctx)
		assert.NoError(t, err)
		assert.Contains(t, labels, "Person")
		assert.Contains(t, labels, "Company")
	})
}

func TestQueryCypher(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	resolver := NewResolver(db)
	qr := &queryResolver{resolver}

	t.Run("executes cypher query", func(t *testing.T) {
		db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Alice"})

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
	resolver := NewResolver(db)
	qr := &queryResolver{resolver}

	t.Run("finds shortest path", func(t *testing.T) {
		// Create a simple graph: A -> B -> C
		a, _ := db.CreateNode(ctx, []string{"Node"}, map[string]interface{}{"name": "A"})
		b, _ := db.CreateNode(ctx, []string{"Node"}, map[string]interface{}{"name": "B"})
		c, _ := db.CreateNode(ctx, []string{"Node"}, map[string]interface{}{"name": "C"})
		db.CreateEdge(ctx, a.ID, b.ID, "CONNECTS", nil)
		db.CreateEdge(ctx, b.ID, c.ID, "CONNECTS", nil)

		path, err := qr.queryShortestPath(ctx, a.ID, c.ID, nil, nil)
		assert.NoError(t, err)
		assert.Len(t, path, 3) // A -> B -> C
	})

	t.Run("returns empty for disconnected nodes", func(t *testing.T) {
		a, _ := db.CreateNode(ctx, []string{"Isolated"}, map[string]interface{}{"name": "X"})
		b, _ := db.CreateNode(ctx, []string{"Isolated"}, map[string]interface{}{"name": "Y"})

		path, err := qr.queryShortestPath(ctx, a.ID, b.ID, nil, nil)
		assert.NoError(t, err)
		assert.Empty(t, path)
	})
}

func TestQueryNeighborhood(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	resolver := NewResolver(db)
	qr := &queryResolver{resolver}

	t.Run("returns neighborhood subgraph", func(t *testing.T) {
		// Create a star graph: center connected to 3 nodes
		center, _ := db.CreateNode(ctx, []string{"Center"}, map[string]interface{}{"name": "Hub"})
		n1, _ := db.CreateNode(ctx, []string{"Leaf"}, map[string]interface{}{"name": "L1"})
		n2, _ := db.CreateNode(ctx, []string{"Leaf"}, map[string]interface{}{"name": "L2"})
		n3, _ := db.CreateNode(ctx, []string{"Leaf"}, map[string]interface{}{"name": "L3"})
		db.CreateEdge(ctx, center.ID, n1.ID, "CONNECTS", nil)
		db.CreateEdge(ctx, center.ID, n2.ID, "CONNECTS", nil)
		db.CreateEdge(ctx, center.ID, n3.ID, "CONNECTS", nil)

		depth := 1
		subgraph, err := qr.queryNeighborhood(ctx, center.ID, &depth, nil, nil, nil)
		assert.NoError(t, err)
		require.NotNil(t, subgraph)
		assert.Len(t, subgraph.Nodes, 4)         // center + 3 leaves
		assert.Len(t, subgraph.Relationships, 3) // 3 edges
	})
}

// =============================================================================
// Mutation Tests
// =============================================================================

func TestMutationCreateNode(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	resolver := NewResolver(db)
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
	resolver := NewResolver(db)
	mr := &mutationResolver{resolver}

	t.Run("updates node properties", func(t *testing.T) {
		// Create node first
		created, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{
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
	resolver := NewResolver(db)
	mr := &mutationResolver{resolver}

	t.Run("deletes existing node", func(t *testing.T) {
		created, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "ToDelete"})

		success, err := mr.mutationDeleteNode(ctx, created.ID)
		assert.NoError(t, err)
		assert.True(t, success)

		// Verify deleted
		_, err = db.GetNode(ctx, created.ID)
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
	resolver := NewResolver(db)
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
	resolver := NewResolver(db)
	mr := &mutationResolver{resolver}

	t.Run("deletes multiple nodes", func(t *testing.T) {
		n1, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		n2, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Bob"})

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
	resolver := NewResolver(db)
	mr := &mutationResolver{resolver}

	t.Run("creates relationship between nodes", func(t *testing.T) {
		n1, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		n2, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Bob"})

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
	resolver := NewResolver(db)
	mr := &mutationResolver{resolver}

	t.Run("deletes existing relationship", func(t *testing.T) {
		n1, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		n2, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Bob"})
		edge, _ := db.CreateEdge(ctx, n1.ID, n2.ID, "KNOWS", nil)

		success, err := mr.mutationDeleteRelationship(ctx, edge.ID)
		assert.NoError(t, err)
		assert.True(t, success)
	})
}

func TestMutationMergeNode(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	resolver := NewResolver(db)
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
		db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{
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
	resolver := NewResolver(db)
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
	resolver := NewResolver(db)
	mr := &mutationResolver{resolver}

	t.Run("requires correct confirmation phrase", func(t *testing.T) {
		_, err := mr.mutationClearAll(ctx, "wrong phrase")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid confirmation phrase")
	})

	t.Run("clears all data with correct phrase", func(t *testing.T) {
		// Create some data
		db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Bob"})

		success, err := mr.mutationClearAll(ctx, "DELETE ALL DATA")
		assert.NoError(t, err)
		assert.True(t, success)

		// Verify cleared
		stats := db.Stats()
		assert.Equal(t, int64(0), stats.NodeCount)
	})
}

func TestMutationRebuildSearchIndex(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	resolver := NewResolver(db)
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
	resolver := NewResolver(db)
	nr := &nodeResolver{resolver}

	t.Run("returns relationships for node", func(t *testing.T) {
		n1, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		n2, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Bob"})
		n3, _ := db.CreateNode(ctx, []string{"Company"}, map[string]interface{}{"name": "TechCorp"})
		db.CreateEdge(ctx, n1.ID, n2.ID, "KNOWS", nil)
		db.CreateEdge(ctx, n1.ID, n3.ID, "WORKS_AT", nil)

		node := &models.Node{ID: n1.ID}
		rels, err := nr.nodeRelationships(ctx, node, nil, nil, nil)
		assert.NoError(t, err)
		assert.Len(t, rels, 2)
	})

	t.Run("filters by type", func(t *testing.T) {
		n1, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Test"})
		n2, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Other"})
		db.CreateEdge(ctx, n1.ID, n2.ID, "KNOWS", nil)
		db.CreateEdge(ctx, n1.ID, n2.ID, "WORKS_WITH", nil)

		node := &models.Node{ID: n1.ID}
		rels, err := nr.nodeRelationships(ctx, node, []string{"KNOWS"}, nil, nil)
		assert.NoError(t, err)
		assert.Len(t, rels, 1)
		assert.Equal(t, "KNOWS", rels[0].Type)
	})

	t.Run("filters by direction", func(t *testing.T) {
		n1, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Center"})
		n2, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Out"})
		n3, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "In"})
		db.CreateEdge(ctx, n1.ID, n2.ID, "OUTGOING", nil) // outgoing from n1
		db.CreateEdge(ctx, n3.ID, n1.ID, "INCOMING", nil) // incoming to n1

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
	resolver := NewResolver(db)
	nr := &nodeResolver{resolver}

	t.Run("returns neighboring nodes", func(t *testing.T) {
		n1, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		n2, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Bob"})
		n3, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Charlie"})
		db.CreateEdge(ctx, n1.ID, n2.ID, "KNOWS", nil)
		db.CreateEdge(ctx, n1.ID, n3.ID, "KNOWS", nil)

		node := &models.Node{ID: n1.ID}
		neighbors, err := nr.nodeNeighbors(ctx, node, nil, nil, nil, nil)
		assert.NoError(t, err)
		assert.Len(t, neighbors, 2)
	})

	t.Run("filters by label", func(t *testing.T) {
		n1, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Center"})
		n2, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "PersonNeighbor"})
		n3, _ := db.CreateNode(ctx, []string{"Company"}, map[string]interface{}{"name": "CompanyNeighbor"})
		db.CreateEdge(ctx, n1.ID, n2.ID, "KNOWS", nil)
		db.CreateEdge(ctx, n1.ID, n3.ID, "WORKS_AT", nil)

		node := &models.Node{ID: n1.ID}
		neighbors, err := nr.nodeNeighbors(ctx, node, nil, nil, []string{"Person"}, nil)
		assert.NoError(t, err)
		assert.Len(t, neighbors, 1)
		assert.Contains(t, neighbors[0].Labels, "Person")
	})
}

// =============================================================================
// Relationship Resolver Tests
// =============================================================================

func TestRelationshipStartNode(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	resolver := NewResolver(db)
	rr := &relationshipResolver{resolver}

	t.Run("returns start node", func(t *testing.T) {
		n1, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		n2, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Bob"})
		db.CreateEdge(ctx, n1.ID, n2.ID, "KNOWS", nil)

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
	resolver := NewResolver(db)
	rr := &relationshipResolver{resolver}

	t.Run("returns end node", func(t *testing.T) {
		n1, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Alice"})
		n2, _ := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{"name": "Bob"})
		db.CreateEdge(ctx, n1.ID, n2.ID, "KNOWS", nil)

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
