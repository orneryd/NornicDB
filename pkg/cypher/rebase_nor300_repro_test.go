package cypher

import (
	"github.com/orneryd/nornicdb/pkg/storage"
	"testing"
)

func TestRebaseBaselineNOR300ExplainNodeKey(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "rebase_nor300"))
	_, err := executor.Execute(t.Context(), "EXPLAIN CREATE CONSTRAINT example_key FOR (n:Example) REQUIRE (n.a, n.b) IS NODE KEY", nil)
	if err != nil {
		t.Fatalf("EXPLAIN of supported constraint rejected: %v", err)
	}
}
