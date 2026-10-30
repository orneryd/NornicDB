package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestUserProcedureRegistrationAndExecution(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)

	err := RegisterUserProcedure(ProcedureSpec{
		Name:        "custom.echo",
		Signature:   "custom.echo(value :: ANY) :: (value :: ANY)",
		Description: "Echoes the first argument",
		Mode:        ProcedureModeRead,
		MinArgs:     1,
		MaxArgs:     1,
	}, func(ctx context.Context, exec *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{
			Columns: []string{"value"},
			Rows:    [][]interface{}{{args[0]}},
		}, nil
	})
	require.NoError(t, err)

	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, "CALL custom.echo('hello') YIELD value RETURN value", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"value"}, result.Columns)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "hello", result.Rows[0][0])

	showResult, err := exec.Execute(ctx, "SHOW PROCEDURES", nil)
	require.NoError(t, err)
	found := false
	for _, row := range showResult.Rows {
		if len(row) > 0 && row[0] == "custom.echo" {
			found = true
			break
		}
	}
	require.True(t, found, "custom.echo should be discoverable via SHOW PROCEDURES")
}

func TestDbmsProceduresIncludesSignatureColumn(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, "CALL dbms.procedures() YIELD name, signature RETURN name, signature", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"name", "signature"}, result.Columns)
	require.NotEmpty(t, result.Rows)
}

func TestCallYieldUnknownColumnReturnsError(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CALL dbms.procedures() YIELD missingColumn RETURN missingColumn", nil)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "unknown YIELD column"))
}
