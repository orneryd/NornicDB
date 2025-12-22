package cypher

import (
"context"
"testing"

"github.com/orneryd/nornicdb/pkg/storage"
)

func TestDiagnosticMergeWorksIn(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Setup minimal data
	exec.Execute(ctx, "CREATE (a:Area {name: 'Area1'})", nil)
	exec.Execute(ctx, "CREATE (a:Area {name: 'Area2'})", nil)
	exec.Execute(ctx, "CREATE (poc:POC {name: 'POC1'})", nil)
	exec.Execute(ctx, "CREATE (p:Person {name: 'Alice'})", nil)
	
	// Link POC to Area1
	exec.Execute(ctx, "MATCH (poc:POC {name: 'POC1'}), (a:Area {name: 'Area1'}) CREATE (poc)-[:BELONGS_TO]->(a)", nil)
	
	// Link Person to POC
	exec.Execute(ctx, "MATCH (p:Person {name: 'Alice'}), (poc:POC {name: 'POC1'}) CREATE (poc)-[:HAS_CONTACT]->(p)", nil)
	
	// Test: Use separate MATCH patterns which works correctly
	t.Log("=== Testing with separate MATCH patterns ===")
	_, err := exec.Execute(ctx, "MATCH (p:Person)<-[:HAS_CONTACT]-(poc:POC) MATCH (poc)-[:BELONGS_TO]->(a:Area) MERGE (p)-[:WORKS_IN]->(a)", nil)
	if err != nil {
		t.Fatalf("MERGE failed: %v", err)
	}
	
	// Check the result
	worksInResult, _ := exec.Execute(ctx, "MATCH (p:Person)-[:WORKS_IN]->(a:Area) RETURN p.name, a.name", nil)
	t.Logf("WORKS_IN relationships created: %d", len(worksInResult.Rows))
	for _, row := range worksInResult.Rows {
		t.Logf("  %v -> %v", row[0], row[1])
	}
	
	if len(worksInResult.Rows) != 1 {
		t.Errorf("Expected 1 WORKS_IN relationship, got %d", len(worksInResult.Rows))
	}
}
