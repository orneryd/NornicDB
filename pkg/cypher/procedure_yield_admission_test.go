package cypher

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcedureYieldAdmissionBeforeNestedAndPipelineHandlers(t *testing.T) {
	const name = "yield_admission.declared"
	calls := nor315Register(t, name, nor315EmptyHandler)
	forEachParser(t, func(t *testing.T) {
		for _, query := range []string{
			"UNWIND [1] AS seed CALL " + name + "() YIELD missing RETURN missing",
			"UNWIND [] AS seed CALL " + name + "() YIELD missing RETURN missing",
			"CALL { CALL " + name + "() YIELD missing RETURN missing } RETURN missing",
			"UNWIND [1] AS seed CALL { WITH seed CALL " + name + "() YIELD missing RETURN missing } RETURN missing",
			"CALL " + name + "() YIELD missing AS value RETURN value",
		} {
			for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
				t.Run(mode+query, func(t *testing.T) {
					exec := newCallScopeExecutor(t)
					before := calls.Load()
					result, err := exec.Execute(t.Context(), mode+query, nil)
					require.ErrorContains(t, err, "missing")
					require.Nil(t, result)
					require.Equal(t, before, calls.Load(), "invalid declared output must not invoke nested/pipeline handlers")
				})
			}
		}
	})
}

func TestProcedureYieldAdmissionPreservesDeclarationBoundaries(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			returns   []ProcedureColumn
			signature string
			declared  bool
		}{
			{name: "absent output metadata"},
			{name: "empty output metadata", returns: []ProcedureColumn{}},
			{name: "empty signature outputs", signature: "() :: ()"},
			{name: "signature-only display metadata", signature: "() :: (value :: INTEGER)"},
			{name: "explicit output declaration", returns: []ProcedureColumn{{Name: "value", Type: "INTEGER"}}, declared: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				const name = "yield_admission.boundary"
				var calls atomic.Int64
				registerProcedureForTest(t, ProcedureSpec{Name: name, Signature: name + tc.signature, Returns: tc.returns, Mode: ProcedureModeRead, MinArgs: 0, MaxArgs: 0}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
					calls.Add(1)
					return &ExecuteResult{Columns: []string{"value"}, Rows: [][]interface{}{{int64(7)}}}, nil
				})
				exec := newCallScopeExecutor(t)
				for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
					before := calls.Load()
					valid, err := exec.Execute(t.Context(), mode+"CALL "+name+"() YIELD value AS kept RETURN kept", nil)
					require.NoError(t, err)
					require.Equal(t, []string{"kept"}, valid.Columns)
					if mode == "EXPLAIN " {
						require.Equal(t, before, calls.Load())
						require.Empty(t, valid.Rows)
					} else {
						require.Equal(t, before+1, calls.Load())
						require.Equal(t, [][]interface{}{{int64(7)}}, valid.Rows)
					}
					before = calls.Load()
					invalid, err := exec.Execute(t.Context(), mode+"CALL "+name+"() YIELD missing RETURN missing", nil)
					if tc.declared {
						require.ErrorContains(t, err, "missing")
						require.Nil(t, invalid)
						require.Equal(t, before, calls.Load())
					} else if mode == "EXPLAIN " {
						require.NoError(t, err)
						require.NotNil(t, invalid)
						require.Equal(t, before, calls.Load())
					} else {
						require.ErrorContains(t, err, "missing")
						require.Nil(t, invalid)
						require.Equal(t, before+1, calls.Load(), "undeclared headers remain checked against actual results")
					}
				}
			})
		}
	})
}

// CREATE PROCEDURE reports a display signature with a value placeholder, while
// its body owns its actual result columns. That is not a closed declaration.
func TestProcedureYieldAdmissionPreservesCreatedProcedureOutputs(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := newCallScopeExecutor(t)
		const name = "yield_admission.created"
		_, err := exec.Execute(t.Context(), "CREATE PROCEDURE "+name+"() MODE READ AS RETURN 17 AS actual", nil)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, err := exec.Execute(context.Background(), "DROP PROCEDURE "+name, nil)
			require.NoError(t, err)
		})
		procedure, found := globalProcedureRegistry.Get(name)
		require.True(t, found)
		require.Empty(t, procedure.Spec.Returns)
		require.Contains(t, procedure.Spec.Signature, "value :: ANY")
		for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
			result, err := exec.Execute(t.Context(), mode+"CALL "+name+"() YIELD actual AS kept RETURN kept", nil)
			require.NoError(t, err)
			require.Equal(t, []string{"kept"}, result.Columns)
			if mode == "EXPLAIN " {
				require.Empty(t, result.Rows)
			} else {
				require.Equal(t, [][]interface{}{{int64(17)}}, result.Rows)
			}
		}
	})
}

func TestProcedureYieldAdmissionPreservesRuntimeHeaderAlternatives(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		const name = "yield_admission.alternatives"
		var calls atomic.Int64
		registerProcedureForTest(t, ProcedureSpec{Name: name, Mode: ProcedureModeRead, MinArgs: 0, MaxArgs: 0, Returns: []ProcedureColumn{{Name: "left", Type: "INTEGER"}, {Name: "right", Type: "INTEGER"}}, ResultSchemas: func() [][]string { return [][]string{{"left"}, {"right"}} }}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
			calls.Add(1)
			return &ExecuteResult{Columns: []string{"left"}, Rows: [][]interface{}{{int64(7)}}}, nil
		})
		exec := newCallScopeExecutor(t)
		for _, column := range []string{"left", "right"} {
			result, err := exec.Execute(t.Context(), "EXPLAIN CALL "+name+"() YIELD "+column+" RETURN "+column, nil)
			require.NoError(t, err)
			require.Equal(t, []string{column}, result.Columns)
		}
		require.Zero(t, calls.Load())
		result, err := exec.Execute(t.Context(), "CALL "+name+"() YIELD left RETURN left", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
		result, err = exec.Execute(t.Context(), "CALL "+name+"() YIELD right RETURN right", nil)
		require.ErrorContains(t, err, "right")
		require.Nil(t, result)
		require.Equal(t, int64(2), calls.Load(), "runtime alternative still validates its actual header")
	})
}

func TestProcedureYieldAdmissionAcceptsBuiltinHeaderAlternatives(t *testing.T) {
	ensureBuiltInProceduresRegistered()
	exec := newCallScopeExecutor(t)
	for _, spec := range globalProcedureRegistry.ListBuiltIns() {
		if spec.ResultSchemas == nil {
			continue
		}
		for _, header := range spec.ResultSchemas() {
			for _, column := range header {
				name := "`" + strings.ReplaceAll(column, "`", "``") + "`"
				columns, err := exec.StatementResultColumns("CALL "+spec.Name+" YIELD "+name, RawSource)
				require.NoError(t, err, spec.Name+" YIELD "+name)
				require.Equal(t, []string{column}, columns)
			}
		}
	}
}
