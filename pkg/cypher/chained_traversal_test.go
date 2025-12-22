// Package cypher tests for chained pattern traversal operations.
// These tests cover multi-segment patterns like (a)<-[:R1]-(b)-[:R2]->(c)
package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupChainedTestData creates a graph for testing chained traversals
// Graph structure:
//
//	Person1 <-[HAS_CONTACT]- POC1 -[BELONGS_TO]-> Area1
//	Person2 <-[HAS_CONTACT]- POC2 -[BELONGS_TO]-> Area1
//	Person3 <-[HAS_CONTACT]- POC3 -[BELONGS_TO]-> Area2
//	Area1 <-[MANAGES]- Manager1
//	Area2 <-[MANAGES]- Manager2
func setupChainedTestData(t *testing.T, exec *StorageExecutor) {
	ctx := context.Background()

	// Create Person nodes
	_, err := exec.Execute(ctx, "CREATE (p:Person {name: 'Person1', email: 'p1@test.com'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (p:Person {name: 'Person2', email: 'p2@test.com'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (p:Person {name: 'Person3', email: 'p3@test.com'})", nil)
	require.NoError(t, err)

	// Create POC nodes
	_, err = exec.Execute(ctx, "CREATE (poc:POC {name: 'POC1', role: 'Technical'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (poc:POC {name: 'POC2', role: 'Business'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (poc:POC {name: 'POC3', role: 'Technical'})", nil)
	require.NoError(t, err)

	// Create Area nodes
	_, err = exec.Execute(ctx, "CREATE (a:Area {name: 'Area1', code: 'A1'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (a:Area {name: 'Area2', code: 'A2'})", nil)
	require.NoError(t, err)

	// Create Manager nodes
	_, err = exec.Execute(ctx, "CREATE (m:Manager {name: 'Manager1'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (m:Manager {name: 'Manager2'})", nil)
	require.NoError(t, err)

	// Get all nodes by label
	persons, _ := exec.storage.GetNodesByLabel("Person")
	pocs, _ := exec.storage.GetNodesByLabel("POC")
	areas, _ := exec.storage.GetNodesByLabel("Area")
	managers, _ := exec.storage.GetNodesByLabel("Manager")

	// Map nodes by name
	nodeByName := make(map[string]*storage.Node)
	for _, n := range persons {
		nodeByName[n.Properties["name"].(string)] = n
	}
	for _, n := range pocs {
		nodeByName[n.Properties["name"].(string)] = n
	}
	for _, n := range areas {
		nodeByName[n.Properties["name"].(string)] = n
	}
	for _, n := range managers {
		nodeByName[n.Properties["name"].(string)] = n
	}

	// Create HAS_CONTACT relationships: POC -> Person
	exec.storage.CreateEdge(&storage.Edge{
		ID:        "hc1",
		Type:      "HAS_CONTACT",
		StartNode: nodeByName["POC1"].ID,
		EndNode:   nodeByName["Person1"].ID,
	})
	exec.storage.CreateEdge(&storage.Edge{
		ID:        "hc2",
		Type:      "HAS_CONTACT",
		StartNode: nodeByName["POC2"].ID,
		EndNode:   nodeByName["Person2"].ID,
	})
	exec.storage.CreateEdge(&storage.Edge{
		ID:        "hc3",
		Type:      "HAS_CONTACT",
		StartNode: nodeByName["POC3"].ID,
		EndNode:   nodeByName["Person3"].ID,
	})

	// Create BELONGS_TO relationships: POC -> Area
	exec.storage.CreateEdge(&storage.Edge{
		ID:        "bt1",
		Type:      "BELONGS_TO",
		StartNode: nodeByName["POC1"].ID,
		EndNode:   nodeByName["Area1"].ID,
	})
	exec.storage.CreateEdge(&storage.Edge{
		ID:        "bt2",
		Type:      "BELONGS_TO",
		StartNode: nodeByName["POC2"].ID,
		EndNode:   nodeByName["Area1"].ID,
	})
	exec.storage.CreateEdge(&storage.Edge{
		ID:        "bt3",
		Type:      "BELONGS_TO",
		StartNode: nodeByName["POC3"].ID,
		EndNode:   nodeByName["Area2"].ID,
	})

	// Create MANAGES relationships: Manager -> Area
	exec.storage.CreateEdge(&storage.Edge{
		ID:        "mg1",
		Type:      "MANAGES",
		StartNode: nodeByName["Manager1"].ID,
		EndNode:   nodeByName["Area1"].ID,
	})
	exec.storage.CreateEdge(&storage.Edge{
		ID:        "mg2",
		Type:      "MANAGES",
		StartNode: nodeByName["Manager2"].ID,
		EndNode:   nodeByName["Area2"].ID,
	})
}

// TestIsChainedPattern tests the detection of chained patterns
func TestIsChainedPattern(t *testing.T) {
	exec := setupTestExecutor(t)

	tests := []struct {
		name      string
		pattern   string
		isChained bool
	}{
		{
			name:      "simple two-node pattern",
			pattern:   "(a:Person)-[:KNOWS]->(b:Person)",
			isChained: false,
		},
		{
			name:      "simple incoming pattern",
			pattern:   "(a)<-[:KNOWS]-(b)",
			isChained: false,
		},
		{
			name:      "three-node chain outgoing",
			pattern:   "(a)-[:R1]->(b)-[:R2]->(c)",
			isChained: true,
		},
		{
			name:      "three-node chain mixed direction",
			pattern:   "(a)<-[:R1]-(b)-[:R2]->(c)",
			isChained: true,
		},
		{
			name:      "four-node chain",
			pattern:   "(a)-[:R1]->(b)-[:R2]->(c)-[:R3]->(d)",
			isChained: true,
		},
		{
			name:      "chain with labels",
			pattern:   "(p:Person)<-[:HAS_CONTACT]-(poc:POC)-[:BELONGS_TO]->(a:Area)",
			isChained: true,
		},
		{
			name:      "chain with properties",
			pattern:   "(p:Person {name: 'Test'})<-[:HAS_CONTACT]-(poc)-[:BELONGS_TO]->(a)",
			isChained: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.isChainedPattern(tt.pattern)
			assert.Equal(t, tt.isChained, result, "isChainedPattern(%q)", tt.pattern)
		})
	}
}

// TestParseChainedTraversalPattern tests parsing of multi-segment patterns
func TestParseChainedTraversalPattern(t *testing.T) {
	exec := setupTestExecutor(t)

	t.Run("three-node chain with variables", func(t *testing.T) {
		pattern := "(p:Person)<-[:HAS_CONTACT]-(poc:POC)-[:BELONGS_TO]->(a:Area)"
		match := exec.parseTraversalPattern(pattern)

		require.NotNil(t, match)
		assert.True(t, match.IsChained)
		assert.Len(t, match.Segments, 2)

		// Check start node
		assert.Equal(t, "p", match.StartNode.variable)
		assert.Contains(t, match.StartNode.labels, "Person")

		// Check end node
		assert.Equal(t, "a", match.EndNode.variable)
		assert.Contains(t, match.EndNode.labels, "Area")

		// Check intermediate nodes
		assert.Len(t, match.IntermediateNodes, 1)
		assert.Equal(t, "poc", match.IntermediateNodes[0].variable)
		assert.Contains(t, match.IntermediateNodes[0].labels, "POC")

		// Check first segment
		assert.Equal(t, "incoming", match.Segments[0].Relationship.Direction)
		assert.Contains(t, match.Segments[0].Relationship.Types, "HAS_CONTACT")

		// Check second segment
		assert.Equal(t, "outgoing", match.Segments[1].Relationship.Direction)
		assert.Contains(t, match.Segments[1].Relationship.Types, "BELONGS_TO")
	})

	t.Run("four-node chain", func(t *testing.T) {
		pattern := "(a)-[:R1]->(b)-[:R2]->(c)-[:R3]->(d)"
		match := exec.parseTraversalPattern(pattern)

		require.NotNil(t, match)
		assert.True(t, match.IsChained)
		assert.Len(t, match.Segments, 3)
		assert.Len(t, match.IntermediateNodes, 2)

		// Check intermediate nodes
		assert.Equal(t, "b", match.IntermediateNodes[0].variable)
		assert.Equal(t, "c", match.IntermediateNodes[1].variable)
	})

	t.Run("chain with relationship variables", func(t *testing.T) {
		pattern := "(a)-[r1:REL1]->(b)-[r2:REL2]->(c)"
		match := exec.parseTraversalPattern(pattern)

		require.NotNil(t, match)
		assert.True(t, match.IsChained)
		assert.Equal(t, "r1", match.Segments[0].Relationship.Variable)
		assert.Equal(t, "r2", match.Segments[1].Relationship.Variable)
	})

	t.Run("chain with properties", func(t *testing.T) {
		pattern := "(p:Person {name: 'Test'})<-[:HAS_CONTACT]-(poc:POC)-[:BELONGS_TO]->(a:Area)"
		match := exec.parseTraversalPattern(pattern)

		require.NotNil(t, match)
		assert.True(t, match.IsChained)
		assert.Equal(t, "Test", match.StartNode.properties["name"])
	})
}

// TestTraverseChainedGraph tests the actual traversal of chained patterns
func TestTraverseChainedGraph(t *testing.T) {
	exec := setupTestExecutor(t)
	setupChainedTestData(t, exec)

	t.Run("three-node chain returns correct paths", func(t *testing.T) {
		pattern := "(p:Person)<-[:HAS_CONTACT]-(poc:POC)-[:BELONGS_TO]->(a:Area)"
		match := exec.parseTraversalPattern(pattern)
		require.NotNil(t, match)

		paths := exec.traverseGraph(match)

		// Should find 3 paths: Person1->POC1->Area1, Person2->POC2->Area1, Person3->POC3->Area2
		assert.Len(t, paths, 3)

		// Each path should have 3 nodes and 2 relationships
		for _, path := range paths {
			assert.Len(t, path.Nodes, 3, "each path should have 3 nodes")
			assert.Len(t, path.Relationships, 2, "each path should have 2 relationships")
		}
	})

	t.Run("filtered chain returns correct subset", func(t *testing.T) {
		// Only get paths to Area1
		pattern := "(p:Person)<-[:HAS_CONTACT]-(poc:POC)-[:BELONGS_TO]->(a:Area {name: 'Area1'})"
		match := exec.parseTraversalPattern(pattern)
		require.NotNil(t, match)

		paths := exec.traverseGraph(match)

		// Should find 2 paths: Person1->POC1->Area1, Person2->POC2->Area1
		assert.Len(t, paths, 2)
	})

	t.Run("non-matching chain returns empty", func(t *testing.T) {
		// Use a non-existent relationship type
		pattern := "(p:Person)<-[:NONEXISTENT]-(poc:POC)-[:BELONGS_TO]->(a:Area)"
		match := exec.parseTraversalPattern(pattern)
		require.NotNil(t, match)

		paths := exec.traverseGraph(match)
		assert.Len(t, paths, 0)
	})
}

// TestBuildPathContextChained tests that buildPathContext correctly maps all nodes in chained patterns
func TestBuildPathContextChained(t *testing.T) {
	exec := setupTestExecutor(t)
	setupChainedTestData(t, exec)

	t.Run("maps all three nodes", func(t *testing.T) {
		pattern := "(p:Person)<-[:HAS_CONTACT]-(poc:POC)-[:BELONGS_TO]->(a:Area)"
		match := exec.parseTraversalPattern(pattern)
		require.NotNil(t, match)

		paths := exec.traverseGraph(match)
		require.NotEmpty(t, paths)

		// Build context for first path
		ctx := exec.buildPathContext(paths[0], match)

		// All three node variables should be mapped
		assert.NotNil(t, ctx.nodes["p"], "start node 'p' should be mapped")
		assert.NotNil(t, ctx.nodes["poc"], "intermediate node 'poc' should be mapped")
		assert.NotNil(t, ctx.nodes["a"], "end node 'a' should be mapped")

		// Verify the nodes are correct types
		pNode := ctx.nodes["p"]
		assert.Contains(t, pNode.Labels, "Person")

		pocNode := ctx.nodes["poc"]
		assert.Contains(t, pocNode.Labels, "POC")

		aNode := ctx.nodes["a"]
		assert.Contains(t, aNode.Labels, "Area")
	})

	t.Run("maps relationships with variables", func(t *testing.T) {
		pattern := "(p:Person)<-[r1:HAS_CONTACT]-(poc:POC)-[r2:BELONGS_TO]->(a:Area)"
		match := exec.parseTraversalPattern(pattern)
		require.NotNil(t, match)

		paths := exec.traverseGraph(match)
		require.NotEmpty(t, paths)

		ctx := exec.buildPathContext(paths[0], match)

		// Both relationship variables should be mapped
		assert.NotNil(t, ctx.rels["r1"], "relationship 'r1' should be mapped")
		assert.NotNil(t, ctx.rels["r2"], "relationship 'r2' should be mapped")

		assert.Equal(t, "HAS_CONTACT", ctx.rels["r1"].Type)
		assert.Equal(t, "BELONGS_TO", ctx.rels["r2"].Type)
	})
}

// TestTraverseFromNode tests the traverseFromNode helper function
func TestTraverseFromNode(t *testing.T) {
	exec := setupTestExecutor(t)
	setupChainedTestData(t, exec)

	t.Run("traverses from specific node", func(t *testing.T) {
		// Get a POC node
		pocs, _ := exec.storage.GetNodesByLabel("POC")
		require.NotEmpty(t, pocs)

		var poc1 *storage.Node
		for _, poc := range pocs {
			if poc.Properties["name"] == "POC1" {
				poc1 = poc
				break
			}
		}
		require.NotNil(t, poc1)

		// Create a simple match pattern
		match := &TraversalMatch{
			StartNode: nodePatternInfo{
				variable: "poc",
				labels:   []string{"POC"},
			},
			EndNode: nodePatternInfo{
				variable: "a",
				labels:   []string{"Area"},
			},
			Relationship: RelationshipPattern{
				Types:     []string{"BELONGS_TO"},
				Direction: "outgoing",
				MinHops:   1,
				MaxHops:   1,
			},
		}

		paths := exec.traverseFromNode(poc1, match)

		// POC1 should connect to Area1
		assert.Len(t, paths, 1)
		assert.Len(t, paths[0].Nodes, 2)
		assert.Equal(t, "POC1", paths[0].Nodes[0].Properties["name"])
		assert.Equal(t, "Area1", paths[0].Nodes[1].Properties["name"])
	})

	t.Run("returns nil for non-matching label", func(t *testing.T) {
		// Get a Person node
		persons, _ := exec.storage.GetNodesByLabel("Person")
		require.NotEmpty(t, persons)

		// Try to traverse from Person but require POC label
		match := &TraversalMatch{
			StartNode: nodePatternInfo{
				variable: "poc",
				labels:   []string{"POC"}, // Person won't match this
			},
			EndNode: nodePatternInfo{
				variable: "a",
				labels:   []string{"Area"},
			},
			Relationship: RelationshipPattern{
				Types:     []string{"BELONGS_TO"},
				Direction: "outgoing",
				MinHops:   1,
				MaxHops:   1,
			},
		}

		paths := exec.traverseFromNode(persons[0], match)
		assert.Nil(t, paths)
	})
}

// TestChainedPatternEndToEnd tests full query execution with chained patterns
func TestChainedPatternEndToEnd(t *testing.T) {
	exec := setupTestExecutor(t)
	setupChainedTestData(t, exec)
	ctx := context.Background()

	t.Run("MATCH with three-node chain", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)<-[:HAS_CONTACT]-(poc:POC)-[:BELONGS_TO]->(a:Area)
			RETURN p.name, poc.name, a.name
		`, nil)

		require.NoError(t, err)
		assert.Len(t, result.Rows, 3)

		// Collect results
		results := make(map[string]string)
		for _, row := range result.Rows {
			personName := row[0].(string)
			areaName := row[2].(string)
			results[personName] = areaName
		}

		// Verify correct mappings
		assert.Equal(t, "Area1", results["Person1"])
		assert.Equal(t, "Area1", results["Person2"])
		assert.Equal(t, "Area2", results["Person3"])
	})

	t.Run("MATCH with WHERE filter on chained pattern", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)<-[:HAS_CONTACT]-(poc:POC)-[:BELONGS_TO]->(a:Area)
			WHERE a.name = 'Area1'
			RETURN p.name, a.name
		`, nil)

		require.NoError(t, err)
		assert.Len(t, result.Rows, 2)

		for _, row := range result.Rows {
			assert.Equal(t, "Area1", row[1].(string))
		}
	})

	t.Run("MATCH with filter on intermediate node", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)<-[:HAS_CONTACT]-(poc:POC)-[:BELONGS_TO]->(a:Area)
			WHERE poc.role = 'Technical'
			RETURN p.name, poc.role, a.name
		`, nil)

		require.NoError(t, err)
		assert.Len(t, result.Rows, 2) // POC1 and POC3 are Technical

		for _, row := range result.Rows {
			assert.Equal(t, "Technical", row[1].(string))
		}
	})

	t.Run("COUNT on chained pattern", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)<-[:HAS_CONTACT]-(poc:POC)-[:BELONGS_TO]->(a:Area)
			RETURN COUNT(p)
		`, nil)

		require.NoError(t, err)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, int64(3), result.Rows[0][0])
	})

	t.Run("GROUP BY on chained pattern", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)<-[:HAS_CONTACT]-(poc:POC)-[:BELONGS_TO]->(a:Area)
			RETURN a.name, COUNT(p) as personCount
		`, nil)

		require.NoError(t, err)
		assert.Len(t, result.Rows, 2) // Area1 and Area2

		counts := make(map[string]int64)
		for _, row := range result.Rows {
			counts[row[0].(string)] = row[1].(int64)
		}

		assert.Equal(t, int64(2), counts["Area1"]) // Person1 and Person2
		assert.Equal(t, int64(1), counts["Area2"]) // Person3
	})
}

// TestChainedPatternEdgeCases tests edge cases for chained patterns
func TestChainedPatternEdgeCases(t *testing.T) {
	exec := setupTestExecutor(t)

	t.Run("empty graph returns no paths", func(t *testing.T) {
		pattern := "(a)-[:R1]->(b)-[:R2]->(c)"
		match := exec.parseTraversalPattern(pattern)
		require.NotNil(t, match)

		paths := exec.traverseGraph(match)
		assert.Len(t, paths, 0)
	})

	t.Run("pattern with quoted property containing special chars", func(t *testing.T) {
		pattern := `(p:Person {name: "Test (User)"})<-[:HAS_CONTACT]-(poc)-[:BELONGS_TO]->(a)`
		match := exec.parseTraversalPattern(pattern)

		require.NotNil(t, match)
		assert.True(t, match.IsChained)
		assert.Equal(t, "Test (User)", match.StartNode.properties["name"])
	})

	t.Run("pattern with no labels on intermediate nodes", func(t *testing.T) {
		pattern := "(a:Start)<-[:R1]-(b)-[:R2]->(c:End)"
		match := exec.parseTraversalPattern(pattern)

		require.NotNil(t, match)
		assert.True(t, match.IsChained)
		assert.Contains(t, match.StartNode.labels, "Start")
		assert.Contains(t, match.EndNode.labels, "End")
		assert.Empty(t, match.IntermediateNodes[0].labels)
	})
}

// TestChainedPatternRegressionBug tests the specific bug where third node was nil
func TestChainedPatternRegressionBug(t *testing.T) {
	// This test verifies the fix for the bug where:
	// MATCH (p:Person)<-[:HAS_CONTACT]-(poc:POC)-[:BELONGS_TO]->(a:Area)
	// would return a=nil because buildPathContext only mapped start/end nodes

	exec := setupTestExecutor(t)
	setupChainedTestData(t, exec)
	ctx := context.Background()

	t.Run("all three node variables are accessible in RETURN", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)<-[:HAS_CONTACT]-(poc:POC)-[:BELONGS_TO]->(a:Area)
			RETURN p, poc, a
		`, nil)

		require.NoError(t, err)
		require.NotEmpty(t, result.Rows)

		for i, row := range result.Rows {
			// All three values should be non-nil
			assert.NotNil(t, row[0], "row %d: p should not be nil", i)
			assert.NotNil(t, row[1], "row %d: poc should not be nil", i)
			assert.NotNil(t, row[2], "row %d: a should not be nil", i)
		}
	})

	t.Run("property access on all three nodes works", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)<-[:HAS_CONTACT]-(poc:POC)-[:BELONGS_TO]->(a:Area)
			WHERE p.name = 'Person1'
			RETURN p.name, poc.name, a.name
		`, nil)

		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		row := result.Rows[0]
		assert.Equal(t, "Person1", row[0])
		assert.Equal(t, "POC1", row[1])
		assert.Equal(t, "Area1", row[2])
	})
}

// TestFourNodeChain tests patterns with four nodes
func TestFourNodeChain(t *testing.T) {
	exec := setupTestExecutor(t)
	setupChainedTestData(t, exec)
	ctx := context.Background()

	t.Run("four-node chain traversal", func(t *testing.T) {
		// Person <- HAS_CONTACT - POC - BELONGS_TO -> Area <- MANAGES - Manager
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)<-[:HAS_CONTACT]-(poc:POC)-[:BELONGS_TO]->(a:Area)<-[:MANAGES]-(m:Manager)
			RETURN p.name, poc.name, a.name, m.name
		`, nil)

		require.NoError(t, err)
		require.NotEmpty(t, result.Rows)

		// Should find paths for all 3 persons
		assert.Len(t, result.Rows, 3)

		// Verify structure
		for _, row := range result.Rows {
			assert.NotNil(t, row[0]) // p.name
			assert.NotNil(t, row[1]) // poc.name
			assert.NotNil(t, row[2]) // a.name
			assert.NotNil(t, row[3]) // m.name
		}
	})
}

// BenchmarkChainedTraversal benchmarks the chained traversal performance
func BenchmarkChainedTraversal(b *testing.B) {
	exec := setupBenchmarkExecutor(b)
	setupChainedBenchmarkData(b, exec)

	pattern := "(p:Person)<-[:HAS_CONTACT]-(poc:POC)-[:BELONGS_TO]->(a:Area)"
	match := exec.parseTraversalPattern(pattern)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = exec.traverseGraph(match)
	}
}

// setupBenchmarkExecutor creates an executor for benchmarks
func setupBenchmarkExecutor(b *testing.B) *StorageExecutor {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	return exec
}

// setupChainedBenchmarkData creates benchmark test data
func setupChainedBenchmarkData(b *testing.B, exec *StorageExecutor) {
	ctx := context.Background()

	// Create 100 persons, 100 POCs, 10 areas
	for i := 0; i < 100; i++ {
		exec.Execute(ctx, fmt.Sprintf("CREATE (p:Person {name: 'Person%d'})", i), nil)
		exec.Execute(ctx, fmt.Sprintf("CREATE (poc:POC {name: 'POC%d'})", i), nil)
	}
	for i := 0; i < 10; i++ {
		exec.Execute(ctx, fmt.Sprintf("CREATE (a:Area {name: 'Area%d'})", i), nil)
	}

	// Get all nodes
	persons, _ := exec.storage.GetNodesByLabel("Person")
	pocs, _ := exec.storage.GetNodesByLabel("POC")
	areas, _ := exec.storage.GetNodesByLabel("Area")

	// Create relationships
	for i, poc := range pocs {
		if i < len(persons) {
			exec.storage.CreateEdge(&storage.Edge{
				ID:        storage.EdgeID(fmt.Sprintf("hc%d", i)),
				Type:      "HAS_CONTACT",
				StartNode: poc.ID,
				EndNode:   persons[i].ID,
			})
		}
		exec.storage.CreateEdge(&storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("bt%d", i)),
			Type:      "BELONGS_TO",
			StartNode: poc.ID,
			EndNode:   areas[i%len(areas)].ID,
		})
	}
}
