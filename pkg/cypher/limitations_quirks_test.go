package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func newTestExecutor(t testing.TB) (*StorageExecutor, storage.Engine) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	return NewStorageExecutor(store), store
}

func TestToLowerWithParameterInContains(t *testing.T) {
	exec, store := newTestExecutor(t)
	ctx := context.Background()

	_, _ = store.CreateNode(&storage.Node{
		ID:     "rx1",
		Labels: []string{"Prescription"},
		Properties: map[string]interface{}{
			"drugName": "Amoxicillin",
		},
	})

	query := `
MATCH (rx:Prescription)
WHERE toLower(rx.drugName) CONTAINS toLower($drugName)
RETURN rx.drugName
`
	result, err := exec.Execute(ctx, query, map[string]interface{}{
		"drugName": "AMOX",
	})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if got, ok := result.Rows[0][0].(string); !ok || got != "Amoxicillin" {
		t.Fatalf("unexpected result: %#v", result.Rows[0][0])
	}
}

func TestOptionalMatchWithParameters(t *testing.T) {
	exec, store := newTestExecutor(t)
	ctx := context.Background()

	_, _ = store.CreateNode(&storage.Node{
		ID:     "m1",
		Labels: []string{"Member"},
		Properties: map[string]interface{}{
			"memberId": "member-1",
		},
	})
	_, _ = store.CreateNode(&storage.Node{
		ID:     "o1",
		Labels: []string{"Order"},
		Properties: map[string]interface{}{
			"orderId": "order-1",
		},
	})
	_ = store.CreateEdge(&storage.Edge{
		ID:        "e1",
		StartNode: "m1",
		EndNode:   "o1",
		Type:      "PLACED_ORDER",
	})

	query := `
MATCH (m:Member {memberId: $memberId})
OPTIONAL MATCH (m)-[:PLACED_ORDER]->(o:Order {orderId: $orderId})
RETURN m.memberId, o.orderId
`
	result, err := exec.Execute(ctx, query, map[string]interface{}{
		"memberId": "member-1",
		"orderId":  "order-1",
	})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "member-1" || result.Rows[0][1] != "order-1" {
		t.Fatalf("unexpected row: %#v", result.Rows[0])
	}

	// When the optional match doesn't find a row, the related columns should be nil.
	result, err = exec.Execute(ctx, query, map[string]interface{}{
		"memberId": "member-1",
		"orderId":  "order-missing",
	})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "member-1" || result.Rows[0][1] != nil {
		t.Fatalf("unexpected optional row: %#v", result.Rows[0])
	}
}

func TestDuplicateRowsAndDistinct(t *testing.T) {
	exec, store := newTestExecutor(t)
	ctx := context.Background()

	_, _ = store.CreateNode(&storage.Node{
		ID:     "m1",
		Labels: []string{"Member"},
		Properties: map[string]interface{}{
			"memberId": "member-1",
		},
	})
	_, _ = store.CreateNode(&storage.Node{ID: "o1", Labels: []string{"Order"}})
	_, _ = store.CreateNode(&storage.Node{ID: "o2", Labels: []string{"Order"}})
	_, _ = store.CreateNode(&storage.Node{
		ID:     "rx1",
		Labels: []string{"Prescription"},
		Properties: map[string]interface{}{
			"drugName": "Amoxicillin",
		},
	})
	_, _ = store.CreateNode(&storage.Node{
		ID:     "p1",
		Labels: []string{"Prescriber"},
		Properties: map[string]interface{}{
			"name": "Dr. Smith",
		},
	})

	_ = store.CreateEdge(&storage.Edge{ID: "e1", StartNode: "m1", EndNode: "o1", Type: "PLACED_ORDER"})
	_ = store.CreateEdge(&storage.Edge{ID: "e2", StartNode: "m1", EndNode: "o2", Type: "PLACED_ORDER"})
	_ = store.CreateEdge(&storage.Edge{ID: "e3", StartNode: "o1", EndNode: "rx1", Type: "CONTAINS"})
	_ = store.CreateEdge(&storage.Edge{ID: "e4", StartNode: "o2", EndNode: "rx1", Type: "CONTAINS"})
	_ = store.CreateEdge(&storage.Edge{ID: "e5", StartNode: "rx1", EndNode: "p1", Type: "PRESCRIBED_BY"})

	query := `
MATCH (m:Member)-[:PLACED_ORDER]->(o:Order)-[:CONTAINS]->(rx:Prescription)-[:PRESCRIBED_BY]->(p:Prescriber)
WHERE m.memberId = $memberId
RETURN p.name, rx.drugName
`
	result, err := exec.Execute(ctx, query, map[string]interface{}{"memberId": "member-1"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows (duplicates), got %d", len(result.Rows))
	}

	queryDistinct := `
MATCH (m:Member)-[:PLACED_ORDER]->(o:Order)-[:CONTAINS]->(rx:Prescription)-[:PRESCRIBED_BY]->(p:Prescriber)
WHERE m.memberId = $memberId
RETURN DISTINCT p.name, rx.drugName
`
	distinctResult, err := exec.Execute(ctx, queryDistinct, map[string]interface{}{"memberId": "member-1"})
	if err != nil {
		t.Fatalf("distinct query failed: %v", err)
	}
	if len(distinctResult.Rows) != 1 {
		t.Fatalf("expected 1 row after DISTINCT, got %d", len(distinctResult.Rows))
	}
}

func TestDoubleMatchClauses(t *testing.T) {
	exec, store := newTestExecutor(t)
	ctx := context.Background()

	_, _ = store.CreateNode(&storage.Node{
		ID:     "m1",
		Labels: []string{"Member"},
		Properties: map[string]interface{}{
			"memberId": "member-1",
		},
	})
	_, _ = store.CreateNode(&storage.Node{ID: "o1", Labels: []string{"Order"}})
	_, _ = store.CreateNode(&storage.Node{
		ID:     "rx1",
		Labels: []string{"Prescription"},
		Properties: map[string]interface{}{
			"drugName": "Amoxicillin",
		},
	})

	_ = store.CreateEdge(&storage.Edge{ID: "e1", StartNode: "m1", EndNode: "o1", Type: "PLACED_ORDER"})
	_ = store.CreateEdge(&storage.Edge{ID: "e2", StartNode: "o1", EndNode: "rx1", Type: "CONTAINS"})

	query := `
MATCH (m:Member)-[:PLACED_ORDER]->(o:Order)
MATCH (o)-[:CONTAINS]->(rx:Prescription)
WHERE m.memberId = $memberId
RETURN rx.drugName
`
	result, err := exec.Execute(ctx, query, map[string]interface{}{"memberId": "member-1"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "Amoxicillin" {
		t.Fatalf("unexpected row: %#v", result.Rows[0])
	}
}
