package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCompatProcedureNamesMatchCanonicalCase(t *testing.T) {
	for _, tc := range []struct {
		query   string
		missing bool
	}{
		{"CALL db.labels()", false},
		{"cAlL db.labels ()", false},
		{"MATCH (n:Missing) CALL db.labels() YIELD label RETURN label", false},
		{"MATCH (n:Missing) CALL db.Labels() YIELD label RETURN label", true},
		{"UNWIND [1] AS x CALL db.Labels() YIELD label RETURN label", true},
		{"CALL db.Labels()", true},
		{"CALL DB.labels()", true},
		{"CALL db.awaitIndexes()", false},
		{"CALL db.awaitindexes()", true},
		{"CALL DB.CDC.QUERY()", true},
	} {
		t.Run(tc.query, func(t *testing.T) {
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "case_audit"))
			_, err := executor.Execute(context.Background(), tc.query, nil)
			if !tc.missing {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			var classified interface{ BoltErrorCode() string }
			require.True(t, errors.As(err, &classified), "%v", err)
			require.Equal(t, "Neo.ClientError.Procedure.ProcedureNotFound", classified.BoltErrorCode())
		})
	}
}

func TestProcedureRegistryPreservesNamesThatDifferByCase(t *testing.T) {
	registry := NewProcedureRegistry()
	for _, name := range []string{"tenant.Mixed", "tenant.mixed"} {
		require.NoError(t, registry.RegisterUser(ProcedureSpec{Name: name, Mode: ProcedureModeRead}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
			return &ExecuteResult{}, nil
		}))
	}
	require.Len(t, registry.List(), 2)
	for _, name := range []string{"tenant.Mixed", "tenant.mixed"} {
		procedure, found := registry.Get(name)
		require.True(t, found)
		require.Equal(t, name, procedure.Spec.Name)
	}
	_, found := registry.Get("tenant.MIXED")
	require.False(t, found)
}
