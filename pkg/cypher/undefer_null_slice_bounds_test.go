package cypher

import (
	"context"
	"testing"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestUndeferNullSliceRuntimeBounds(t *testing.T) {
	for _, query := range []string{
		"CREATE (n:R1 {k:1.0}) RETURN null[n.k..] AS v",
		"CREATE (n:R1 {k:1.0}) RETURN [1,2][null..n.k] AS v",
	} {
		t.Run(query, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "null-bounds"))
			result, err := exec.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{nil}}, result.Rows)
		})
	}
	for _, query := range []string{
		"CREATE (n:R1 {z:0}) RETURN null[1/n.z..] AS v",
		"CREATE (n:R1 {z:0}) RETURN [1,2][null..1/n.z] AS v",
	} {
		t.Run(query, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "null-errors"))
			_, err := exec.Execute(context.Background(), query, nil)
			requireRound3Neo4jError(t, err, "/ by zero")
		})
	}
}
