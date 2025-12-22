package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// CREATE with Inline Node Definitions
// =============================================================================

// TestCreateInlineNodeInRelationship tests creating a new node inline within a relationship pattern
func TestCreateInlineNodeInRelationship(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes and relationship in one CREATE statement with inline definitions
	result, err := exec.Execute(ctx, `CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})`, nil)
	require.NoError(t, err)

	assert.Equal(t, 2, result.Stats.NodesCreated, "Should create 2 nodes")
	assert.Equal(t, 1, result.Stats.RelationshipsCreated, "Should create 1 relationship")

	// Verify nodes exist
	nodes, _ := store.GetNodesByLabel("Person")
	assert.Len(t, nodes, 2)
}

// TestMatchCreateWithInlineNode tests MATCH followed by CREATE with inline node definition
func TestMatchCreateWithInlineNode(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// First create a person
	_, err := exec.Execute(ctx, `CREATE (p:Person {name: 'Alice'})`, nil)
	require.NoError(t, err)

	// MATCH the person and CREATE a company with relationship using inline definition
	result, err := exec.Execute(ctx, `
		MATCH (p:Person {name: 'Alice'})
		CREATE (c:Company {name: 'Acme'})-[:EMPLOYS]->(p)
	`, nil)
	require.NoError(t, err)

	assert.Equal(t, 1, result.Stats.NodesCreated, "Should create 1 company node")
	assert.Equal(t, 1, result.Stats.RelationshipsCreated, "Should create 1 relationship")

	// Verify company exists
	companies, _ := store.GetNodesByLabel("Company")
	assert.Len(t, companies, 1)
	assert.Equal(t, "Acme", companies[0].Properties["name"])
}

// TestMatchCreateWithInlineNodeReverse tests CREATE with inline node in reverse direction
func TestMatchCreateWithInlineNodeReverse(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// First create a company
	_, err := exec.Execute(ctx, `CREATE (c:Company {name: 'Acme'})`, nil)
	require.NoError(t, err)

	// MATCH the company and CREATE a person with relationship in reverse direction
	result, err := exec.Execute(ctx, `
		MATCH (c:Company {name: 'Acme'})
		CREATE (c)<-[:WORKS_AT]-(p:Person {name: 'Bob'})
	`, nil)
	require.NoError(t, err)

	assert.Equal(t, 1, result.Stats.NodesCreated, "Should create 1 person node")
	assert.Equal(t, 1, result.Stats.RelationshipsCreated, "Should create 1 relationship")

	// Verify relationship direction
	companies, _ := store.GetNodesByLabel("Company")
	persons, _ := store.GetNodesByLabel("Person")

	edges, _ := store.GetIncomingEdges(companies[0].ID)
	assert.Len(t, edges, 1)
	assert.Equal(t, "WORKS_AT", edges[0].Type)
	assert.Equal(t, persons[0].ID, edges[0].StartNode)
}

// TestMatchCreateBothInline tests CREATE with inline nodes on both sides
func TestMatchCreateBothInline(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create two new nodes with a relationship between them (no MATCH)
	result, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:FRIEND_OF]->(b:Person {name: 'Bob'})
	`, nil)
	require.NoError(t, err)

	assert.Equal(t, 2, result.Stats.NodesCreated, "Should create 2 nodes")
	assert.Equal(t, 1, result.Stats.RelationshipsCreated, "Should create 1 relationship")
}

// TestMatchCreateWithInlineAndVariable tests mixing inline definition with variable reference
func TestMatchCreateWithInlineAndVariable(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create initial nodes
	_, err := exec.Execute(ctx, `CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})`, nil)
	require.NoError(t, err)

	// MATCH one node, CREATE inline node connecting to matched node
	result, err := exec.Execute(ctx, `
		MATCH (a:Person {name: 'Alice'})
		CREATE (c:Company {name: 'TechCorp'})-[:EMPLOYS]->(a)
	`, nil)
	require.NoError(t, err)

	assert.Equal(t, 1, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)

	// Verify company was created and connected
	companies, _ := store.GetNodesByLabel("Company")
	assert.Len(t, companies, 1)
}

// TestMatchCreateMultipleInlineRelationships tests creating multiple relationships with inline nodes
func TestMatchCreateMultipleInlineRelationships(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a person
	_, err := exec.Execute(ctx, `CREATE (p:Person {name: 'Alice'})`, nil)
	require.NoError(t, err)

	// Create multiple relationships with inline nodes
	result, err := exec.Execute(ctx, `
		MATCH (p:Person {name: 'Alice'})
		CREATE (c1:Company {name: 'Corp1'})-[:EMPLOYS]->(p)
		CREATE (c2:Company {name: 'Corp2'})-[:CONTRACTED]->(p)
	`, nil)
	require.NoError(t, err)

	assert.Equal(t, 2, result.Stats.NodesCreated, "Should create 2 company nodes")
	assert.Equal(t, 2, result.Stats.RelationshipsCreated, "Should create 2 relationships")
}

// TestCreateInlineWithProperties tests inline node with complex properties
func TestCreateInlineWithProperties(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `
		CREATE (p:Person {name: 'Alice', age: 30, active: true})-[:WORKS_AT]->(c:Company {name: 'Acme', founded: 1990})
	`, nil)
	require.NoError(t, err)

	assert.Equal(t, 2, result.Stats.NodesCreated)
	assert.Equal(t, 1, result.Stats.RelationshipsCreated)

	// Verify properties were set
	persons, _ := store.GetNodesByLabel("Person")
	require.Len(t, persons, 1)
	assert.Equal(t, "Alice", persons[0].Properties["name"])
	// Age can be int64 or float64 depending on how it was parsed
	age := persons[0].Properties["age"]
	switch v := age.(type) {
	case int64:
		assert.Equal(t, int64(30), v)
	case float64:
		assert.Equal(t, float64(30), v)
	default:
		t.Errorf("unexpected age type: %T", age)
	}

	companies, _ := store.GetNodesByLabel("Company")
	require.Len(t, companies, 1)
	assert.Equal(t, "Acme", companies[0].Properties["name"])
}

// =============================================================================
// CREATE Patterns - Comprehensive Coverage
// =============================================================================

// TestCreateSingleNode tests basic single node creation
func TestCreateSingleNodeComprehensive(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		labels   []string
		propKey  string
		propVal  interface{}
	}{
		{
			name:    "simple node with label",
			query:   `CREATE (n:Person)`,
			labels:  []string{"Person"},
			propKey: "",
		},
		{
			name:    "node with label and property",
			query:   `CREATE (n:Person {name: 'Alice'})`,
			labels:  []string{"Person"},
			propKey: "name",
			propVal: "Alice",
		},
		{
			name:    "node with multiple properties",
			query:   `CREATE (n:Person {name: 'Bob', age: 25})`,
			labels:  []string{"Person"},
			propKey: "name",
			propVal: "Bob",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseStore := storage.NewMemoryEngine()

			store := storage.NewNamespacedEngine(baseStore, "test")
			exec := NewStorageExecutor(store)
			ctx := context.Background()

			result, err := exec.Execute(ctx, tt.query, nil)
			require.NoError(t, err)
			assert.Equal(t, 1, result.Stats.NodesCreated)

			nodes, _ := store.GetNodesByLabel(tt.labels[0])
			require.Len(t, nodes, 1)

			if tt.propKey != "" {
				assert.Equal(t, tt.propVal, nodes[0].Properties[tt.propKey])
			}
		})
	}
}

// TestCreateMultipleNodes tests creating multiple nodes in various patterns
func TestCreateMultipleNodesComprehensive(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		expectedNodes int
	}{
		{
			name:          "two nodes comma separated",
			query:         `CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})`,
			expectedNodes: 2,
		},
		{
			name:          "three nodes comma separated",
			query:         `CREATE (a:A), (b:B), (c:C)`,
			expectedNodes: 3,
		},
		{
			name:          "nodes with different labels",
			query:         `CREATE (p:Person {name: 'Alice'}), (c:Company {name: 'Acme'})`,
			expectedNodes: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseStore := storage.NewMemoryEngine()

			store := storage.NewNamespacedEngine(baseStore, "test")
			exec := NewStorageExecutor(store)
			ctx := context.Background()

			result, err := exec.Execute(ctx, tt.query, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedNodes, result.Stats.NodesCreated)
		})
	}
}

// TestCreateRelationshipsComprehensive tests various relationship creation patterns
func TestCreateRelationshipsComprehensive(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		expectedNodes int
		expectedRels  int
	}{
		{
			name:          "simple forward relationship",
			query:         `CREATE (a:A)-[:REL]->(b:B)`,
			expectedNodes: 2,
			expectedRels:  1,
		},
		{
			name:          "simple reverse relationship",
			query:         `CREATE (a:A)<-[:REL]-(b:B)`,
			expectedNodes: 2,
			expectedRels:  1,
		},
		{
			name:          "relationship with properties",
			query:         `CREATE (a:A)-[:REL {weight: 1.5}]->(b:B)`,
			expectedNodes: 2,
			expectedRels:  1,
		},
		{
			name:          "chain of relationships",
			query:         `CREATE (a:A)-[:R1]->(b:B), (b)-[:R2]->(c:C)`,
			expectedNodes: 3,
			expectedRels:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseStore := storage.NewMemoryEngine()

			store := storage.NewNamespacedEngine(baseStore, "test")
			exec := NewStorageExecutor(store)
			ctx := context.Background()

			result, err := exec.Execute(ctx, tt.query, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedNodes, result.Stats.NodesCreated, "nodes created")
			assert.Equal(t, tt.expectedRels, result.Stats.RelationshipsCreated, "relationships created")
		})
	}
}

// =============================================================================
// MATCH...CREATE Patterns
// =============================================================================

// TestMatchCreatePatterns tests various MATCH...CREATE combinations
func TestMatchCreatePatterns(t *testing.T) {
	tests := []struct {
		name          string
		setup         string
		query         string
		expectedNodes int
		expectedRels  int
	}{
		{
			name:          "match node create relationship to new node",
			setup:         `CREATE (p:Person {name: 'Alice'})`,
			query:         `MATCH (p:Person {name: 'Alice'}) CREATE (p)-[:KNOWS]->(f:Person {name: 'Friend'})`,
			expectedNodes: 1,
			expectedRels:  1,
		},
		{
			name:          "match node create inline node with relationship",
			setup:         `CREATE (p:Person {name: 'Alice'})`,
			query:         `MATCH (p:Person {name: 'Alice'}) CREATE (c:Company {name: 'Corp'})-[:EMPLOYS]->(p)`,
			expectedNodes: 1,
			expectedRels:  1,
		},
		{
			name:          "match two nodes create relationship between them",
			setup:         `CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})`,
			query:         `MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:FRIENDS]->(b)`,
			expectedNodes: 0,
			expectedRels:  1,
		},
		{
			name:          "match create reverse relationship",
			setup:         `CREATE (p:Person {name: 'Alice'})`,
			query:         `MATCH (p:Person {name: 'Alice'}) CREATE (p)<-[:FOLLOWS]-(f:Person {name: 'Follower'})`,
			expectedNodes: 1,
			expectedRels:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseStore := storage.NewMemoryEngine()

			store := storage.NewNamespacedEngine(baseStore, "test")
			exec := NewStorageExecutor(store)
			ctx := context.Background()

			// Run setup
			if tt.setup != "" {
				_, err := exec.Execute(ctx, tt.setup, nil)
				require.NoError(t, err)
			}

			// Run test query
			result, err := exec.Execute(ctx, tt.query, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedNodes, result.Stats.NodesCreated, "nodes created")
			assert.Equal(t, tt.expectedRels, result.Stats.RelationshipsCreated, "relationships created")
		})
	}
}

// =============================================================================
// MERGE Patterns
// =============================================================================

// TestMergePatterns tests various MERGE scenarios
func TestMergePatterns(t *testing.T) {
	tests := []struct {
		name           string
		setup          string
		query          string
		expectedCreate bool // true if MERGE should create, false if it should match
	}{
		{
			name:           "merge creates new node",
			setup:          "",
			query:          `MERGE (p:Person {name: 'Alice'})`,
			expectedCreate: true,
		},
		{
			name:           "merge matches existing node",
			setup:          `CREATE (p:Person {name: 'Alice'})`,
			query:          `MERGE (p:Person {name: 'Alice'})`,
			expectedCreate: false,
		},
		{
			name:           "merge with ON CREATE SET",
			setup:          "",
			query:          `MERGE (p:Person {name: 'Bob'}) ON CREATE SET p.created = true`,
			expectedCreate: true,
		},
		{
			name:           "merge with ON MATCH SET",
			setup:          `CREATE (p:Person {name: 'Carol'})`,
			query:          `MERGE (p:Person {name: 'Carol'}) ON MATCH SET p.found = true`,
			expectedCreate: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseStore := storage.NewMemoryEngine()

			store := storage.NewNamespacedEngine(baseStore, "test")
			exec := NewStorageExecutor(store)
			ctx := context.Background()

			// Run setup
			if tt.setup != "" {
				_, err := exec.Execute(ctx, tt.setup, nil)
				require.NoError(t, err)
			}

			// Run test query
			result, err := exec.Execute(ctx, tt.query, nil)
			require.NoError(t, err)

			if tt.expectedCreate {
				assert.Equal(t, 1, result.Stats.NodesCreated, "MERGE should create node")
			} else {
				assert.Equal(t, 0, result.Stats.NodesCreated, "MERGE should match existing node")
			}
		})
	}
}

// =============================================================================
// Edge Cases and Error Handling
// =============================================================================

// TestCreateWithParameterizedProperties tests CREATE with parameter substitution
func TestCreateWithParameterizedProperties(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	params := map[string]interface{}{
		"name": "Alice",
		"age":  30,
	}

	result, err := exec.Execute(ctx, `CREATE (p:Person {name: $name, age: $age})`, params)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)

	nodes, _ := store.GetNodesByLabel("Person")
	require.Len(t, nodes, 1)
	assert.Equal(t, "Alice", nodes[0].Properties["name"])
	// Age can be int or float depending on how parameter was processed
	age := nodes[0].Properties["age"]
	switch v := age.(type) {
	case int:
		assert.Equal(t, 30, v)
	case int64:
		assert.Equal(t, int64(30), v)
	case float64:
		assert.Equal(t, float64(30), v)
	default:
		t.Errorf("unexpected age type: %T", age)
	}
}

// TestCreateVariableReuse tests that variables can be reused correctly
func TestCreateVariableReuse(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create node, then use variable in another CREATE
	result, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})
		CREATE (a)-[:SELF_REF]->(a)
	`, nil)
	require.NoError(t, err)

	assert.Equal(t, 1, result.Stats.NodesCreated, "Should create 1 node")
	assert.Equal(t, 1, result.Stats.RelationshipsCreated, "Should create 1 self-referential relationship")
}

// TestMatchCreateDoesNotDuplicateExistingVariable tests that referencing existing variable doesn't create new node
func TestMatchCreateDoesNotDuplicateExistingVariable(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create initial node
	_, err := exec.Execute(ctx, `CREATE (p:Person {name: 'Alice'})`, nil)
	require.NoError(t, err)

	// MATCH and CREATE relationship - should NOT create duplicate Person
	result, err := exec.Execute(ctx, `
		MATCH (p:Person {name: 'Alice'})
		CREATE (p)-[:KNOWS]->(f:Person {name: 'Friend'})
	`, nil)
	require.NoError(t, err)

	assert.Equal(t, 1, result.Stats.NodesCreated, "Should only create the Friend node")

	// Verify we have exactly 2 Person nodes total
	nodes, _ := store.GetNodesByLabel("Person")
	assert.Len(t, nodes, 2, "Should have exactly 2 Person nodes")
}
