package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
)

func TestProcedureArityAdmissionAcrossExecutionModes(t *testing.T) {
	for _, prefix := range []string{"", "UNWIND [] AS ignored "} {
		for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
			t.Run(mode+prefix, func(t *testing.T) {
				executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "procedure_arity_modes"))
				_, err := executor.Execute(t.Context(), mode+prefix+"CALL db.labels(1) YIELD label RETURN label", nil)
				if err == nil {
					t.Fatal("invalid procedure arity was accepted")
				}
			})
		}
	}
}

func TestProcedureMetadataAdmissionDoesNotInvokeHandlers(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		replaceProcedureRegistryForTest(t, NewProcedureRegistry())
		var invocations atomic.Int64
		require.NoError(t, globalProcedureRegistry.RegisterUser(ProcedureSpec{
			Name: "custom.arityProbe", MinArgs: 1, MaxArgs: 1,
			Params:  []ProcedureParam{{Name: "value", Type: "INTEGER"}},
			Returns: []ProcedureColumn{{Name: "value", Type: "INTEGER"}},
		}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
			invocations.Add(1)
			return &ExecuteResult{Columns: []string{"value"}, Rows: [][]interface{}{{int64(7)}}}, nil
		}))
		executor, _ := newTestExecutor(t)
		for _, query := range []string{
			"EXPLAIN CALL custom.arityProbe(7) YIELD value RETURN value",
			"EXPLAIN UNWIND [] AS ignored CALL custom.arityProbe(7) YIELD value RETURN value",
		} {
			result, err := executor.Execute(t.Context(), query, nil)
			require.NoError(t, err, query)
			require.Equal(t, []string{"value"}, result.Columns)
			require.Empty(t, result.Rows)
		}
		for _, query := range []string{
			"CALL custom.arityProbe() YIELD value RETURN value",
			"EXPLAIN CALL custom.arityProbe() YIELD value RETURN value",
			"PROFILE UNWIND [] AS ignored CALL custom.arityProbe() YIELD value RETURN value",
		} {
			_, err := executor.Execute(t.Context(), query, nil)
			requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
		}
		require.Zero(t, invocations.Load(), "planning and rejected statements must not invoke handlers")
		result, err := executor.Execute(t.Context(), "CALL custom.arityProbe(7) YIELD value RETURN value", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
		require.Equal(t, int64(1), invocations.Load())
	})
}

func TestExplainProcedureArityWithoutExecution(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "procedure_arity_explain"))
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
