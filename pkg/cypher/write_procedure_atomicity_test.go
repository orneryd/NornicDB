package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestWriteProcedureStatementAtomicity(t *testing.T) {
	failure := errors.New("u5 deliberate failure after write")
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{
		Name: "u5tx.writefail", Signature: "u5tx.writefail() :: (value :: INTEGER)",
		Mode: ProcedureModeWrite, Returns: []ProcedureColumn{{Name: "value", Type: "INTEGER"}},
	}, func(ctx context.Context, exec *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
		_, err := exec.Execute(ctx, "CREATE (:U5AtomicWrite) RETURN 1 AS value", nil)
		if err != nil {
			return nil, err
		}
		return nil, failure
	}))
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{
		Name: "u5tx.fail", Signature: "u5tx.fail() :: (rejected :: INTEGER)",
		Mode: ProcedureModeRead, Returns: []ProcedureColumn{{Name: "rejected", Type: "INTEGER"}},
	}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		return nil, failure
	}))
	for _, query := range []string{
		"CALL u5tx.writefail() YIELD value RETURN value",
		"CALL apoc.cypher.doIt($statement,{}) YIELD value CALL u5tx.fail() YIELD rejected RETURN value",
	} {
		t.Run(query, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "u5_atomic")
			exec := NewStorageExecutor(store)
			_, err := exec.Execute(context.Background(), query, map[string]interface{}{
				"statement": "CREATE (:U5AtomicWrite) RETURN 1 AS value",
			})
			require.ErrorContains(t, err, failure.Error())
			result, err := NewStorageExecutor(store).Execute(context.Background(), "MATCH (n:U5AtomicWrite) RETURN count(n)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows, "failed procedure statement must roll back its writes")
		})
	}
}
