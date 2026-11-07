package cypher

import (
	"github.com/orneryd/nornicdb/pkg/storage"
	"reflect"
	"testing"
)

func TestRebaseBaselineNOR300ExplainNodeKey(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "rebase_nor300"))
	_, err := executor.Execute(t.Context(), "EXPLAIN CREATE CONSTRAINT example_key FOR (n:Example) REQUIRE (n.a, n.b) IS NODE KEY", nil)
	if err != nil {
		t.Fatalf("EXPLAIN of supported constraint rejected: %v", err)
	}
}

func TestNOR300ExplainNodeKeyDoesNotMutateSchema(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor300_explain"))
	before, err := executor.Execute(t.Context(), "SHOW CONSTRAINTS", nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Execute(t.Context(), "EXPLAIN CREATE CONSTRAINT example_key FOR (n:Example) REQUIRE (n.a, n.b) IS NODE KEY", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Columns) != 0 || len(result.Rows) != 0 {
		t.Fatalf("unexpected EXPLAIN result: %#v", result)
	}
	after, err := executor.Execute(t.Context(), "SHOW CONSTRAINTS", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before.Rows, after.Rows) {
		t.Fatalf("EXPLAIN changed schema: before=%v after=%v", before.Rows, after.Rows)
	}
	for _, invalid := range []string{
		"EXPLAIN CREATE CONSTRAINT bad FOR (n:Example) REQUIRE () IS NODE KEY",
		"EXPLAIN CREATE CONSTRAINT bad FOR (n:Example) REQUIRE (n.a) IS NODE KEY OPTIONS 7",
	} {
		if _, err := executor.Execute(t.Context(), invalid, nil); err == nil {
			t.Errorf("invalid constraint was accepted: %s", invalid)
		}
	}
}
