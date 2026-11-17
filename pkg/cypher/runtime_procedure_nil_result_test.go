package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	nilResultProcedureName        = "custom.nilResult"
	rowResultProcedureName        = "custom.nilResultRows"
	builtInNilResultProcedureName = "custom.builtinNilResult"
	failingProcedureName          = "custom.nilResultFailure"
	writeNilResultProcedureName   = "custom.nilResultWrite"
	nilResultWriteLabel           = "NilResultWrite"
)

// registerProcedureForTest publishes one user procedure for the duration of a
// test. The shared scoped cleanup owns restoring the registry afterwards.
func registerProcedureForTest(t *testing.T, spec ProcedureSpec, handler ProcedureHandler) {
	t.Helper()
	scopeProcedureRegistration(t, spec.Name, userProcedureTable)
	require.NoError(t, RegisterUserProcedure(spec, handler))
}

// registerBuiltInProcedureForTest publishes one built-in registration for the
// duration of a test. It scopes that registration in the built-in table, so a
// previous exact registration under the same name is restored afterwards.
func registerBuiltInProcedureForTest(t *testing.T, spec ProcedureSpec, handler ProcedureHandler) {
	t.Helper()
	scopeProcedureRegistration(t, spec.Name, builtInProcedureTable)
	require.NoError(t, globalProcedureRegistry.RegisterBuiltIn(spec, handler))
}

func nilResultProcedureSpec(name string) ProcedureSpec {
	return ProcedureSpec{
		Name:      name,
		Signature: name + "() :: (value :: INTEGER)",
		Mode:      ProcedureModeRead,
		MinArgs:   0,
		MaxArgs:   0,
	}
}

// registerNilResultProcedure publishes a registered procedure whose handler
// returns no result at all: (nil, nil).
func registerNilResultProcedure(t *testing.T) {
	t.Helper()
	registerProcedureForTest(t, nilResultProcedureSpec(nilResultProcedureName),
		func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
			return nil, nil
		})
}

// assertEmptyProcedureResult runs query and requires the declared empty result:
// the procedure's columns and no rows, never a missing result.
func assertEmptyProcedureResult(t *testing.T, exec *StorageExecutor, query string, columns []string) {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.NotNil(t, result, "a procedure call always serves a result")
	require.Equal(t, columns, result.Columns)
	require.Empty(t, result.Rows)
}

// A registered procedure that returns (nil, nil) means an empty result, not a
// missing one: CALL of it serves the declared columns with no rows, so YIELD
// and a query tail consume a result instead of a nil pointer. The row-returning
// control keeps its own rows and an unknown YIELD column still fails.
func TestCallProcedureNilResultServesDeclaredEmptyResult(t *testing.T) {
	registerNilResultProcedure(t)
	exec := newCallScopeExecutor(t)

	for _, tc := range []struct {
		name    string
		query   string
		columns []string
	}{
		{
			name:    "no yield",
			query:   "CALL " + nilResultProcedureName + "()",
			columns: []string{"value"},
		},
		{
			name:    "yield",
			query:   "CALL " + nilResultProcedureName + "() YIELD value RETURN value",
			columns: []string{"value"},
		},
		{
			name:    "yield where",
			query:   "CALL " + nilResultProcedureName + "() YIELD value WHERE value > 0 RETURN value",
			columns: []string{"value"},
		},
		{
			name:    "yield tail",
			query:   "CALL " + nilResultProcedureName + "() YIELD value WITH value RETURN *",
			columns: []string{"value"},
		},
		{
			name:    "in query pipeline",
			query:   "UNWIND [1, 2] AS seed CALL " + nilResultProcedureName + "() YIELD value RETURN seed, value",
			columns: []string{"seed", "value"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertEmptyProcedureResult(t, exec, tc.query, tc.columns)
		})
	}

	t.Run("unknown yield column still fails", func(t *testing.T) {
		_, err := exec.Execute(context.Background(),
			"CALL "+nilResultProcedureName+"() YIELD missing RETURN missing", nil)
		require.Error(t, err)
	})

	t.Run("nil result keeps its statement", func(t *testing.T) {
		result, err := exec.Execute(context.Background(),
			"UNWIND [0] AS seed CALL "+nilResultProcedureName+"() YIELD value RETURN count(*) AS total", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"total"}, result.Columns)
		require.Len(t, result.Rows, 1)
		require.Equal(t, int64(0), result.Rows[0][0])
	})
}

// A built-in registration takes the same result owner, so its handler's
// (nil, nil) is the declared empty result too.
func TestCallBuiltInProcedureNilResultServesDeclaredEmptyResult(t *testing.T) {
	registerBuiltInProcedureForTest(t, nilResultProcedureSpec(builtInNilResultProcedureName),
		func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
			return nil, nil
		})
	exec := newCallScopeExecutor(t)

	assertEmptyProcedureResult(t, exec, "CALL "+builtInNilResultProcedureName+"()", []string{"value"})
	assertEmptyProcedureResult(t, exec,
		"CALL "+builtInNilResultProcedureName+"() YIELD value RETURN value", []string{"value"})
}

// A handler that reports a failure still fails the call: the empty-result
// contract applies only to a handler that reports no failure.
func TestCallProcedureHandlerErrorIsNotAnEmptyResult(t *testing.T) {
	registerProcedureForTest(t, nilResultProcedureSpec(failingProcedureName),
		func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
			return nil, errors.New("handler failed")
		})
	exec := newCallScopeExecutor(t)

	result, err := exec.Execute(context.Background(), "CALL "+failingProcedureName+"()", nil)
	require.ErrorContains(t, err, "handler failed")
	require.Nil(t, result)
}

// A procedure that does return rows is unaffected by the empty-result
// contract: its rows, columns and values stay its own.
func TestCallProcedureRowsResultControl(t *testing.T) {
	registerProcedureForTest(t, nilResultProcedureSpec(rowResultProcedureName),
		func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
			return &ExecuteResult{
				Columns: []string{"value"},
				Rows:    [][]interface{}{{int64(7)}},
			}, nil
		})
	exec := newCallScopeExecutor(t)

	result, err := exec.Execute(context.Background(),
		"CALL "+rowResultProcedureName+"() YIELD value RETURN value", nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, []string{"value"}, result.Columns)
	require.Len(t, result.Rows, 1)
	require.Equal(t, int64(7), result.Rows[0][0])

	untyped, err := exec.Execute(context.Background(), "CALL "+rowResultProcedureName+"()", nil)
	require.NoError(t, err)
	require.NotNil(t, untyped)
	require.Equal(t, []string{"value"}, untyped.Columns)
	require.Len(t, untyped.Rows, 1)
	require.Equal(t, int64(7), untyped.Rows[0][0])
}

// A write procedure whose handler successfully executes one nested CREATE and
// then returns (nil, nil) still serves its declared empty result, and the
// statement statistics keep the write the handler actually performed: the
// empty result replaces a missing result, never the work behind it.
func TestCallWriteProcedureNilResultKeepsStatementStatistics(t *testing.T) {
	registerProcedureForTest(t, ProcedureSpec{
		Name:               writeNilResultProcedureName,
		Signature:          writeNilResultProcedureName + "() :: (value :: INTEGER)",
		Mode:               ProcedureModeWrite,
		ExecutesStatements: true,
		MinArgs:            0,
		MaxArgs:            0,
	}, func(ctx context.Context, nested *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
		_, err := nested.executeUserStatement(ctx, "CREATE (:"+nilResultWriteLabel+")", nil)
		return nil, err
	})
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)

	result, err := exec.Execute(context.Background(), "CALL "+writeNilResultProcedureName+"()", nil)
	require.NoError(t, err)
	require.NotNil(t, result, "a write procedure call always serves a result")
	require.Equal(t, []string{"value"}, result.Columns)
	require.Empty(t, result.Rows)
	require.NotNil(t, result.Stats, "the actual write keeps its statement statistics")
	require.Equal(t, 1, result.Stats.NodesCreated)

	nodes, err := store.GetNodesByLabel(context.Background(), nilResultWriteLabel)
	require.NoError(t, err)
	require.Len(t, nodes, 1, "the handler's nested CREATE was executed")
}
