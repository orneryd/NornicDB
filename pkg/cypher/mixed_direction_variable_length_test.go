package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestMixedDirectionVariableLengthMatchAfterRelationshipReversal(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(baseStore, "test"))
	ctx := context.Background()
	requireTerminalNames := func(label string, rows [][]interface{}) {
		t.Helper()
		got := make(map[string]int, len(rows))
		for _, row := range rows {
			if len(row) != 1 {
				t.Fatalf("%s returned malformed row %#v", label, row)
			}
			name, ok := row[0].(string)
			if !ok {
				t.Fatalf("%s returned non-string terminal name %#v", label, row[0])
			}
			got[name]++
		}
		want := map[string]int{
			"leftLeaf0":  1,
			"leftLeaf1":  1,
			"rightLeaf0": 1,
			"rightLeaf1": 1,
		}
		if len(got) != len(want) {
			t.Fatalf("%s returned terminal names %#v, want %#v", label, got, want)
		}
		for name, count := range want {
			if got[name] != count {
				t.Fatalf("%s returned terminal names %#v, want %#v", label, got, want)
			}
		}
	}

	_, err := exec.Execute(ctx, `
		CREATE (root:A {name: 'root'}),
		       (left:B {name: 'left'}), (right:B {name: 'right'}),
		       (leftChild:C {name: 'leftChild'}), (rightChild:C {name: 'rightChild'}),
		       (leftLeaf:D {name: 'leftLeaf'}), (rightLeaf:D {name: 'rightLeaf'})
		CREATE (root)-[:LIKES]->(left), (root)-[:LIKES]->(right),
		       (left)-[:LIKES]->(leftChild), (right)-[:LIKES]->(rightChild),
		       (leftChild)-[:LIKES]->(leftLeaf), (rightChild)-[:LIKES]->(rightLeaf)
	`, nil)
	if err != nil {
		t.Fatalf("create tree: %v", err)
	}

	_, err = exec.Execute(ctx, `
		MATCH (source)-[relationship]->(target)
		WHERE NOT source:A
		DELETE relationship
		CREATE (target)-[:LIKES]->(source)
	`, nil)
	if err != nil {
		t.Fatalf("reverse non-root relationships: %v", err)
	}

	_, err = exec.Execute(ctx, `
		MATCH (leaf:D)
		CREATE (first:E {name: leaf.name + '0'}), (second:E {name: leaf.name + '1'})
		CREATE (leaf)-[:LIKES]->(first), (leaf)-[:LIKES]->(second)
	`, nil)
	if err != nil {
		t.Fatalf("create terminal nodes: %v", err)
	}

	chained, err := exec.Execute(ctx, `
		MATCH (root:A)-[:LIKES]->()<-[:LIKES*3]->(terminal)
		RETURN terminal.name
	`, nil)
	if err != nil {
		t.Fatalf("execute single-clause mixed-direction match: %v", err)
	}
	requireTerminalNames("single-clause match", chained.Rows)

	result, err := exec.Execute(ctx, `
		MATCH (root:A)
		MATCH (root)-[:LIKES]->()<-[:LIKES*3]->(terminal)
		RETURN terminal.name
	`, nil)
	if err != nil {
		t.Fatalf("execute mixed-direction match: %v", err)
	}
	requireTerminalNames("bound multi-clause match", result.Rows)
}
