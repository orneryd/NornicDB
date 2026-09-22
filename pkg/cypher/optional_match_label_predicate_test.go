package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestOptionalMatchFiltersCandidatesByLabelPredicate(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(baseStore, "test"))
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
		CREATE (a {name: 'A'}), (b:B {name: 'B'}), (c:C {name: 'C'}), (d:D {name: 'C'})
		CREATE (a)-[:T]->(b), (a)-[:T]->(c), (a)-[:T]->(d)
	`, nil)
	if err != nil {
		t.Fatalf("setup query failed: %v", err)
	}

	result, err := exec.Execute(ctx, `
		MATCH (a)-->(b)
		WHERE b:B
		OPTIONAL MATCH (a)-->(c)
		WHERE c:C
		RETURN a.name
	`, nil)
	if err != nil {
		t.Fatalf("optional match query failed: %v", err)
	}
	if len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != "A" {
		t.Fatalf("expected one filtered row for A, got %#v", result.Rows)
	}
}
