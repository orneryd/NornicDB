package cypher

import (
	"github.com/orneryd/nornicdb/pkg/storage"
	"testing"
)

func TestRebaseBaselineNOR54ProcedureArity(t *testing.T) {
	for _, prefix := range []string{"", "UNWIND [] AS ignored "} {
		for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
			t.Run(mode+prefix, func(t *testing.T) {
				executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "rebase_nor54"))
				_, err := executor.Execute(t.Context(), mode+prefix+"CALL db.labels(1) YIELD label RETURN label", nil)
				if err == nil {
					t.Fatal("invalid procedure arity was accepted")
				}
			})
		}
	}
}
