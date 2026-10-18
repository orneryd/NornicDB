package replication_test

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestReplicatedNamespaceRelationshipCounts(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	for namespace, creates := range map[string]int{"a": 1, "b": 2} {
		writer := cypher.NewStorageExecutor(storage.NewNamespacedEngine(base, namespace))
		for i := 0; i < creates; i++ {
			_, err := writer.Execute(context.Background(), "CREATE (:Start)-[:T]->(:End)", nil)
			require.NoError(t, err)
		}
	}
	inner := storage.NewTracedEngine(storage.NewWALEngine(base, nil))
	reader := replication.NewReplicatedEngine(inner, nil, 0)
	for namespace, want := range map[string]int64{"a": 1, "b": 2} {
		exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(reader, namespace))
		for _, shape := range []string{"()-[r:T]->()", "(:Start)-[r:T]->()", "()-[r:T]->(:End)"} {
			result, err := exec.Execute(context.Background(), "MATCH "+shape+" RETURN count(r)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{want}}, result.Rows)
		}
	}
}
