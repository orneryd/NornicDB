// Package cypher tests for graph traversal operations.
package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// setupTraversalTestData creates a small graph for testing traversals
// Graph structure:
//
//	Alice -[KNOWS]-> Bob -[KNOWS]-> Carol
//	Alice -[WORKS_WITH]-> Carol
//	Bob -[FOLLOWS]-> Dave
func setupTraversalTestData(t *testing.T, exec *StorageExecutor) {
	ctx := context.Background()

	// Create nodes
	_, err := exec.Execute(ctx, "CREATE (a:Person {name: 'Alice', age: 30})", nil)
	if err != nil {
		t.Fatalf("Failed to create Alice: %v", err)
	}
	_, err = exec.Execute(ctx, "CREATE (b:Person {name: 'Bob', age: 25})", nil)
	if err != nil {
		t.Fatalf("Failed to create Bob: %v", err)
	}
	_, err = exec.Execute(ctx, "CREATE (c:Person {name: 'Carol', age: 35})", nil)
	if err != nil {
		t.Fatalf("Failed to create Carol: %v", err)
	}
	_, err = exec.Execute(ctx, "CREATE (d:Person {name: 'Dave', age: 28})", nil)
	if err != nil {
		t.Fatalf("Failed to create Dave: %v", err)
	}

	// Create relationships using direct storage
	// Alice -> Bob (KNOWS)
	alice, _ := exec.storage.GetNodesByLabel("Person")
	var aliceNode, bobNode, carolNode, daveNode *storage.Node
	for _, n := range alice {
		switch n.Properties["name"] {
		case "Alice":
			aliceNode = n
		case "Bob":
			bobNode = n
		case "Carol":
			carolNode = n
		case "Dave":
			daveNode = n
		}
	}

	if aliceNode != nil && bobNode != nil {
		exec.storage.CreateEdge(&storage.Edge{
			ID:        "e1",
			Type:      "KNOWS",
			StartNode: aliceNode.ID,
			EndNode:   bobNode.ID,
		})
	}
	if bobNode != nil && carolNode != nil {
		exec.storage.CreateEdge(&storage.Edge{
			ID:        "e2",
			Type:      "KNOWS",
			StartNode: bobNode.ID,
			EndNode:   carolNode.ID,
		})
	}
	if aliceNode != nil && carolNode != nil {
		exec.storage.CreateEdge(&storage.Edge{
			ID:        "e3",
			Type:      "WORKS_WITH",
			StartNode: aliceNode.ID,
			EndNode:   carolNode.ID,
		})
	}
	if bobNode != nil && daveNode != nil {
		exec.storage.CreateEdge(&storage.Edge{
			ID:        "e4",
			Type:      "FOLLOWS",
			StartNode: bobNode.ID,
			EndNode:   daveNode.ID,
		})
	}
}

func TestParseRelationshipPattern(t *testing.T) {
	exec := setupTestExecutor(t)

	tests := []struct {
		pattern   string
		direction string
		types     []string
		variable  string
		minHops   int
		maxHops   int
	}{
		{"-[r:KNOWS]->", "outgoing", []string{"KNOWS"}, "r", 1, 1},
		{"<-[r:KNOWS]-", "incoming", []string{"KNOWS"}, "r", 1, 1},
		{"-[r]-", "both", nil, "r", 1, 1},
		{"-[:KNOWS]->", "outgoing", []string{"KNOWS"}, "", 1, 1},
		{"-[r:KNOWS|FOLLOWS]->", "outgoing", []string{"KNOWS", "FOLLOWS"}, "r", 1, 1},
		{"-[*1..3]->", "outgoing", nil, "", 1, 3},
		{"-[r*2..5:KNOWS]->", "outgoing", []string{"KNOWS"}, "r", 2, 5},
		{"-[*]->", "outgoing", nil, "", 1, VarLengthUnboundedMaxHops}, // unbounded var-length
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			ctx := context.Background()

			result := exec.parseRelationshipPattern(ctx, tt.pattern)

			if result.Direction != tt.direction {
				t.Errorf("Direction = %s, want %s", result.Direction, tt.direction)
			}
			if len(result.Types) != len(tt.types) {
				t.Errorf("Types = %v, want %v", result.Types, tt.types)
			}
			if result.Variable != tt.variable {
				t.Errorf("Variable = %s, want %s", result.Variable, tt.variable)
			}
			if result.MinHops != tt.minHops {
				t.Errorf("MinHops = %d, want %d", result.MinHops, tt.minHops)
			}
			if result.MaxHops != tt.maxHops {
				t.Errorf("MaxHops = %d, want %d", result.MaxHops, tt.maxHops)
			}
		})
	}
}

func TestParseTraversalPattern(t *testing.T) {
	exec := setupTestExecutor(t)

	tests := []struct {
		pattern      string
		startVar     string
		startLabels  []string
		relDirection string
		relTypes     []string
		endVar       string
		endLabels    []string
	}{
		{
			"(a:Person)-[r:KNOWS]->(b:Person)",
			"a", []string{"Person"},
			"outgoing", []string{"KNOWS"},
			"b", []string{"Person"},
		},
		{
			"(a)<-[r:FOLLOWS]-(b)",
			"a", nil,
			"incoming", []string{"FOLLOWS"},
			"b", nil,
		},
		{
			"(n:User {name: 'Alice'})-[]->(m)",
			"n", []string{"User"},
			"outgoing", nil,
			"m", nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			ctx := context.Background()
			result := exec.parseTraversalPattern(ctx, tt.pattern)
			if result == nil {
				t.Fatal("parseTraversalPattern returned nil")
			}

			if result.StartNode.variable != tt.startVar {
				t.Errorf("StartNode.variable = %s, want %s", result.StartNode.variable, tt.startVar)
			}
			if result.EndNode.variable != tt.endVar {
				t.Errorf("EndNode.variable = %s, want %s", result.EndNode.variable, tt.endVar)
			}
			if result.Relationship.Direction != tt.relDirection {
				t.Errorf("Relationship.Direction = %s, want %s", result.Relationship.Direction, tt.relDirection)
			}
		})
	}
}

func TestMatchWithRelationships(t *testing.T) {
	exec := setupTestExecutor(t)
	setupTraversalTestData(t, exec)
	ctx := context.Background()

	tests := []struct {
		name     string
		query    string
		wantRows int
	}{
		{
			"Simple outgoing KNOWS",
			"MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name",
			2, // Alice->Bob, Bob->Carol
		},
		{
			"All outgoing relationships",
			"MATCH (a:Person)-[r]->(b:Person) RETURN a.name, r, b.name",
			4, // All 4 edges
		},
		{
			"Filtered by relationship type",
			"MATCH (a:Person)-[r:WORKS_WITH]->(b:Person) RETURN a.name, b.name",
			1, // Alice->Carol
		},
		{
			"Incoming relationships",
			"MATCH (a:Person)<-[r:KNOWS]-(b:Person) RETURN a.name, b.name",
			2, // Bob<-Alice, Carol<-Bob
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, nil)
			if err != nil {
				t.Fatalf("Execute failed: %v", err)
			}
			if len(result.Rows) != tt.wantRows {
				t.Errorf("Got %d rows, want %d", len(result.Rows), tt.wantRows)
			}
		})
	}
}

func TestTypeFunction(t *testing.T) {
	exec := setupTestExecutor(t)
	setupTraversalTestData(t, exec)
	ctx := context.Background()

	// Query for relationship type
	result, err := exec.Execute(ctx, "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN type(r), a.name, b.name", nil)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected at least one row")
	}

	// Check that type(r) returns "KNOWS"
	for _, row := range result.Rows {
		if row[0] != "KNOWS" {
			t.Errorf("type(r) = %v, want 'KNOWS'", row[0])
		}
	}
}

func TestShortestPath(t *testing.T) {
	exec := setupTestExecutor(t)
	setupTraversalTestData(t, exec)

	// Get nodes
	people, _ := exec.storage.GetNodesByLabel("Person")
	var alice, carol *storage.Node
	for _, n := range people {
		switch n.Properties["name"] {
		case "Alice":
			alice = n
		case "Carol":
			carol = n
		}
	}

	if alice == nil || carol == nil {
		t.Fatal("Could not find Alice and Carol nodes")
	}

	// Test shortest path Alice -> Carol
	// Direct: Alice -[WORKS_WITH]-> Carol (length 1)
	// Via Bob: Alice -[KNOWS]-> Bob -[KNOWS]-> Carol (length 2)
	path, err := exec.shortestPath(context.Background(), alice, carol, nil, "outgoing", 10)
	if err != nil {
		t.Fatalf("shortestPath: %v", err)
	}

	if path == nil {
		t.Fatal("shortestPath returned nil")
	}

	// Should find the direct WORKS_WITH path (length 1)
	if path.Length != 1 {
		t.Errorf("shortestPath length = %d, want 1", path.Length)
	}

	// Test shortest path with type filter
	pathKnows, err := exec.shortestPath(context.Background(), alice, carol, []string{"KNOWS"}, "outgoing", 10)
	if err != nil {
		t.Fatalf("shortestPath KNOWS: %v", err)
	}
	if pathKnows == nil {
		t.Fatal("shortestPath with KNOWS filter returned nil")
	}

	// Should find Alice -> Bob -> Carol (length 2)
	if pathKnows.Length != 2 {
		t.Errorf("shortestPath KNOWS length = %d, want 2", pathKnows.Length)
	}
}

func TestAllShortestPaths(t *testing.T) {
	exec := setupTestExecutor(t)
	setupTraversalTestData(t, exec)

	// Get nodes
	people, _ := exec.storage.GetNodesByLabel("Person")
	var alice, carol *storage.Node
	for _, n := range people {
		switch n.Properties["name"] {
		case "Alice":
			alice = n
		case "Carol":
			carol = n
		}
	}

	if alice == nil || carol == nil {
		t.Fatal("Could not find Alice and Carol nodes")
	}

	// All shortest paths from Alice to Carol
	paths, err := exec.allShortestPaths(context.Background(), alice, carol, nil, "outgoing", 10)
	if err != nil {
		t.Fatalf("allShortestPaths: %v", err)
	}

	// Should find at least the direct path
	if len(paths) == 0 {
		t.Fatal("allShortestPaths returned empty")
	}

	// All returned paths should have the same (shortest) length
	shortestLen := paths[0].Length
	for i, p := range paths {
		if p.Length != shortestLen {
			t.Errorf("Path %d has length %d, expected %d", i, p.Length, shortestLen)
		}
	}
}

func TestVariableLengthPaths(t *testing.T) {
	exec := setupTestExecutor(t)
	setupTraversalTestData(t, exec)

	// Parse a variable length pattern
	ctx := context.Background()

	pattern := exec.parseRelationshipPattern(ctx, "-[*1..2:KNOWS]->")

	if pattern.MinHops != 1 {
		t.Errorf("MinHops = %d, want 1", pattern.MinHops)
	}
	if pattern.MaxHops != 2 {
		t.Errorf("MaxHops = %d, want 2", pattern.MaxHops)
	}
	if len(pattern.Types) != 1 || pattern.Types[0] != "KNOWS" {
		t.Errorf("Types = %v, want [KNOWS]", pattern.Types)
	}
}

func TestDegreeFunctionsWithTraversal(t *testing.T) {
	exec := setupTestExecutor(t)
	setupTraversalTestData(t, exec)

	// Get Bob's node
	people, _ := exec.storage.GetNodesByLabel("Person")
	var bob *storage.Node
	for _, n := range people {
		if n.Properties["name"] == "Bob" {
			bob = n
			break
		}
	}

	if bob == nil {
		t.Fatal("Could not find Bob node")
	}
	ctx := context.Background()

	// Bob has: 1 incoming (from Alice), 2 outgoing (to Carol, to Dave)
	nodes := map[string]*storage.Node{"n": bob}

	inDegree := exec.evaluateExpressionWithContext(ctx, "inDegree(n)", nodes, nil)
	if inDegree != int64(1) {
		t.Errorf("inDegree(bob) = %v, want 1", inDegree)
	}

	outDegree := exec.evaluateExpressionWithContext(ctx, "outDegree(n)", nodes, nil)
	if outDegree != int64(2) {
		t.Errorf("outDegree(bob) = %v, want 2", outDegree)
	}

	totalDegree := exec.evaluateExpressionWithContext(ctx, "degree(n)", nodes, nil)
	if totalDegree != int64(3) {
		t.Errorf("degree(bob) = %v, want 3", totalDegree)
	}
}
