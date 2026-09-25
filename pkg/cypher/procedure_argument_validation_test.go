package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestProcedureArgumentsRejectKnownIncompatibleTypes(t *testing.T) {
	spec := ProcedureSpec{
		Name:    "custom.acceptInteger",
		Params:  []ProcedureParam{{Name: "value", Type: "INTEGER", Optional: true}},
		MinArgs: 1,
		MaxArgs: 1,
	}

	_, err := extractProcedureInvocationArguments(context.Background(), spec, "CALL custom.acceptInteger(true)")
	require.Error(t, err)

	var semanticError *SemanticError
	require.ErrorAs(t, err, &semanticError)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", semanticError.Code)
	require.Equal(t, "InvalidArgumentType", semanticError.Detail)
}

func TestProcedureArgumentsCoerceIntegerToFloat(t *testing.T) {
	spec := ProcedureSpec{
		Name:    "custom.acceptFloat",
		Params:  []ProcedureParam{{Name: "value", Type: "FLOAT", Optional: true}},
		MinArgs: 1,
		MaxArgs: 1,
	}

	args, err := extractProcedureInvocationArguments(context.Background(), spec, "CALL custom.acceptFloat(42)")
	require.NoError(t, err)
	require.Equal(t, []interface{}{float64(42)}, args)
}

func TestProcedureArgumentsAcceptNumericSubtypeAssignments(t *testing.T) {
	spec := ProcedureSpec{
		Name:    "custom.acceptNumber",
		Params:  []ProcedureParam{{Name: "value", Type: "NUMBER", Optional: true}},
		MinArgs: 1,
		MaxArgs: 1,
	}

	integerArgs, err := extractProcedureInvocationArguments(context.Background(), spec, "CALL custom.acceptNumber(42)")
	require.NoError(t, err)
	require.Equal(t, []interface{}{int64(42)}, integerArgs)

	floatArgs, err := extractProcedureInvocationArguments(context.Background(), spec, "CALL custom.acceptNumber(42.5)")
	require.NoError(t, err)
	require.Equal(t, []interface{}{float64(42.5)}, floatArgs)
}

func TestInQueryProcedureCallRejectsImplicitArguments(t *testing.T) {
	replaceProcedureRegistryForTest(t, NewProcedureRegistry())
	require.NoError(t, globalProcedureRegistry.RegisterUser(
		ProcedureSpec{
			Name:    "custom.acceptInteger",
			Params:  []ProcedureParam{{Name: "value", Type: "INTEGER", Optional: true}},
			Returns: []ProcedureColumn{{Name: "out", Type: "INTEGER"}},
			MinArgs: 1,
			MaxArgs: 1,
		},
		func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
			return &ExecuteResult{Columns: []string{"out"}}, nil
		},
	))

	exec := &StorageExecutor{}
	_, err := exec.executeCall(context.Background(), "CALL custom.acceptInteger YIELD out RETURN out")
	require.Error(t, err)

	var semanticError *SemanticError
	require.ErrorAs(t, err, &semanticError)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", semanticError.Code)
	require.Equal(t, "InvalidArgumentPassingMode", semanticError.Detail)
}

func TestProcedureYieldRejectsDuplicateBindings(t *testing.T) {
	yield := &yieldClause{items: []yieldItem{
		{name: "first", alias: "value"},
		{name: "second", alias: "value"},
	}}

	err := validateProcedureYieldBindings(yield, true)
	require.Error(t, err)

	var semanticError *SemanticError
	require.ErrorAs(t, err, &semanticError)
	require.Equal(t, "VariableAlreadyBound", semanticError.Detail)
}

func TestProcedureYieldAllIsLimitedToStandaloneCalls(t *testing.T) {
	err := validateProcedureYieldBindings(&yieldClause{yieldAll: true}, true)
	require.Error(t, err)

	var semanticError *SemanticError
	require.ErrorAs(t, err, &semanticError)
	require.Equal(t, "UnexpectedSyntax", semanticError.Detail)
	require.NoError(t, validateProcedureYieldBindings(&yieldClause{yieldAll: true}, false))
}

func TestCallTailReturnAllPreservesColumnsAndValues(t *testing.T) {
	exec := &StorageExecutor{}
	seed := &ExecuteResult{
		Columns: []string{"first", "second"},
		Rows:    [][]interface{}{{int64(1), "one"}, {int64(2), "two"}},
	}

	result, err := exec.executeCallTail(context.Background(), seed, "RETURN *")
	require.NoError(t, err)
	require.Equal(t, seed.Columns, result.Columns)
	require.Equal(t, seed.Rows, result.Rows)
}

func TestYieldReturnAllPreservesColumnsAndValues(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	result, err := exec.Execute(context.Background(), "CALL dbms.components() YIELD name, edition RETURN *", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.ElementsMatch(t, []string{"name", "edition"}, result.Columns)
}
