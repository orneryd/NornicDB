package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRelationshipStatementPublicTransactionScript(t *testing.T) {
	for _, prefix := range []string{"BEGIN ", "BEGIN TRANSACTION "} {
		t.Run(prefix, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "relationship_script_source"))
			_, err := exec.Execute(context.Background(), "CREATE ()-[:T {p:1}]->()", nil)
			require.NoError(t, err)
			result, err := exec.Execute(context.Background(), prefix+"MATCH ()-[r:T]->() WHERE r.p=1 RETURN r.p AS p COMMIT", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
		})
	}
}
