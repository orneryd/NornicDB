package cypher_test

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Validation belongs to the statement, including empty input and nested CALLs.
// An earlier UNION branch must not commit before a later size is rejected.
func TestTransactionalCallBatchSizePreflight(t *testing.T) {
	for _, query := range []string{
		"UNWIND [] AS x CALL { WITH x CREATE (:BatchValidationRecord {v:x}) } IN TRANSACTIONS OF 0 ROWS",
		"UNWIND [1] AS x CALL { WITH x CREATE (:BatchValidationRecord {v:x}) } IN TRANSACTIONS OF x ROWS",
		"UNWIND [1] AS x CALL { WITH x CREATE (:BatchValidationRecord {v:x}) } IN TRANSACTIONS OF ROWS",
		"UNWIND [1] AS x CALL { WITH x CREATE (:BatchValidationRecord {v:x}) } IN TRANSACTIONS OF 1/0 ROWS",
		"CALL { UNWIND [1] AS x CALL { WITH x CREATE (:BatchValidationRecord {v:x}) } IN TRANSACTIONS OF 0 ROWS }",
		"CALL { CREATE (:BatchValidationRecord) RETURN 1 AS x } RETURN x UNION ALL CALL { CREATE (:BatchValidationRecord) } IN TRANSACTIONS OF 0 ROWS RETURN 1 AS x",
	} {
		t.Run(query, func(t *testing.T) {
			base, err := storage.NewBadgerEngineInMemory()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(base, "tenant"))
			_, err = exec.Execute(context.Background(), query, nil)
			require.Error(t, err)
			result, err := exec.Execute(context.Background(), "MATCH (n:BatchValidationRecord) RETURN n", nil)
			require.NoError(t, err)
			require.Empty(t, result.Rows)
		})
	}
}
