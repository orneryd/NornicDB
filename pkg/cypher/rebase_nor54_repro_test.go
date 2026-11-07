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

func TestNOR54ExplainProcedureArityControls(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor54_arity"))
	for _, query := range []string{
		"EXPLAIN CALL db.labels() YIELD label RETURN label",
		"EXPLAIN UNWIND [] AS ignored CALL db.labels() YIELD label RETURN label",
	} {
		if _, err := executor.Execute(t.Context(), query, nil); err != nil {
			t.Errorf("valid query %q: %v", query, err)
		}
	}
	query := "EXPLAIN CALL { CALL db.labels(1) YIELD label RETURN label } RETURN label"
	if _, err := executor.Execute(t.Context(), query, nil); err == nil {
		t.Errorf("invalid nested procedure arity accepted: %s", query)
	}
}
