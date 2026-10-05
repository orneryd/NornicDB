package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// registerCoverageProcedures registers cov.echo(value) :: (echoed), which
// yields its argument, and cov.fail(value) :: (echoed), which fails, for the
// test's duration.
func registerCoverageProcedures(t *testing.T) {
	t.Helper()
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)
	spec := func(name string) ProcedureSpec {
		return ProcedureSpec{
			Name:      name,
			Signature: name + "(value :: ANY) :: (echoed :: ANY)",
			Mode:      ProcedureModeRead,
			Params:    []ProcedureParam{{Name: "value", Type: "ANY"}},
			Returns:   []ProcedureColumn{{Name: "echoed", Type: "ANY"}},
			MinArgs:   1,
			MaxArgs:   1,
		}
	}
	require.NoError(t, RegisterUserProcedure(spec("cov.echo"), func(ctx context.Context, exec *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{Columns: []string{"echoed"}, Rows: [][]interface{}{{args[0]}}}, nil
	}))
	require.NoError(t, RegisterUserProcedure(spec("cov.fail"), func(ctx context.Context, exec *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
		return nil, errors.New("cov.fail failed")
	}))
}

// TestPipelineProcedureCallPerRow: a procedure call in a query runs once
// per row with the row's values, and its YIELD's WHERE filters the extended
// rows, as in Neo4j.
func TestPipelineProcedureCallPerRow(t *testing.T) {
	registerCoverageProcedures(t)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	result, err := exec.Execute(context.Background(), "UNWIND ['a', 'b', 'c'] AS x CALL cov.echo(x) YIELD echoed WHERE echoed <> 'b' RETURN x, echoed ORDER BY x", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a", "a"}, {"c", "c"}}, result.Rows)

	_, err = exec.Execute(context.Background(), "UNWIND ['a'] AS x CALL cov.fail(x) YIELD echoed RETURN echoed", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cov.fail failed")
}

// TestPipelineApplyProcedureCallEdges: the pipeline's procedure call
// rejects a non-void call without YIELD and aggregate arguments, and fails when
// an argument or the call fails.
func TestPipelineApplyProcedureCallEdges(t *testing.T) {
	registerCoverageProcedures(t)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	rows := []pipelineRow{{"x": int64(1)}}

	out, _, ok, err := exec.pipelineApplyProcedureCall(ctx, rows, "CALL cov.echo(x)")
	requireSyntaxErrorStatus(t, err, "non-void procedure without explicit YIELD")
	require.True(t, ok)
	require.Nil(t, out)

	_, _, ok, err = exec.pipelineApplyProcedureCall(ctx, rows, "CALL cov.echo(count(x)) YIELD echoed")
	require.True(t, ok)
	require.Error(t, err)
	require.Contains(t, err.Error(), "aggregate")

	_, _, ok, err = exec.pipelineApplyProcedureCall(ctx, rows, "CALL cov.echo(x % 0) YIELD echoed")
	require.True(t, ok)
	require.Error(t, err)
	require.Contains(t, err.Error(), "/ by zero")

	_, _, ok, err = exec.pipelineApplyProcedureCall(ctx, rows, "CALL cov.fail(x) YIELD echoed")
	require.True(t, ok)
	require.Error(t, err)

	out, yielded, ok, err := exec.pipelineApplyProcedureCall(ctx, []pipelineRow{{"x": int64(1)}, {"x": int64(2)}}, "CALL cov.echo(x) YIELD echoed WHERE echoed > 1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []string{"echoed"}, yielded)
	require.Equal(t, []pipelineRow{{"x": int64(2), "echoed": int64(2)}}, out)
}

func TestExplicitProcedureArgumentTexts_EmptyAndNested(t *testing.T) {
	arguments := explicitProcedureArgumentTexts("CALL db.labels")
	require.Nil(t, arguments)

	arguments = explicitProcedureArgumentTexts("CALL cov.echo(1")
	require.Nil(t, arguments)

	arguments = explicitProcedureArgumentTexts("CALL cov.echo()")
	require.NotNil(t, arguments)
	require.Empty(t, arguments)
	arguments = explicitProcedureArgumentTexts("CALL cov.echo(1, [2, 3])")
	require.Equal(t, []string{"1", "[2, 3]"}, arguments)
}

// TestProcedureCallValidationErrors: an aggregate procedure argument and a
// YIELD WHERE after ORDER BY / SKIP / LIMIT are SyntaxErrors; a YIELD SKIP /
// LIMIT is checked as a WITH's is.
func TestProcedureCallValidationErrors(t *testing.T) {
	registerCoverageProcedures(t)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()

	spec, found := globalProcedureRegistry.Get("cov.echo")
	require.True(t, found)
	_, err := extractProcedureInvocationArguments(ctx, spec.Spec, "CALL cov.echo(count(1))")
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.ClientError.Statement.SyntaxError")

	yield := parseYieldClause("CALL cov.echo(1) YIELD echoed ORDER BY echoed WHERE echoed = 1")
	require.NotNil(t, yield)
	err = exec.validateYieldModifiers(yield, true)
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.ClientError.Statement.SyntaxError")

	for _, call := range []string{
		"CALL cov.echo(1) YIELD echoed SKIP -1",
		"CALL cov.echo(1) YIELD echoed LIMIT -1",
	} {
		yield := parseYieldClause(call)
		require.NotNil(t, yield, call)
		require.Error(t, exec.validateYieldModifiers(yield, true), call)
	}
}
