package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCreateParameterDoesNotInjectAsyncBatchClauses(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "parameter_injection"))
	ctx := context.Background()

	result, err := executor.Execute(ctx, "CREATE (:PT {r: $r})", map[string]interface{}{
		"r": "CREATE (m:Injected {secret: 1})",
	})
	require.NoError(t, err)
	require.Equal(t, 1, result.Stats.NodesCreated)

	nodes, err := executor.getStorage(ctx).GetNodesByLabel("PT")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, "CREATE (m:Injected {secret: 1})", nodes[0].Properties["r"])

	injected, err := executor.getStorage(ctx).GetNodesByLabel("Injected")
	require.NoError(t, err)
	require.Empty(t, injected)
}

func TestCreateParameterDoesNotInjectRelationshipPattern(t *testing.T) {
	payload := "a})-[:X]->(b"

	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "implicit", true: "explicit"}[explicit], func(t *testing.T) {
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "relationship_parameter_injection"))
			ctx := context.Background()
			if explicit {
				_, err := executor.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}

			_, err := executor.Execute(ctx, "CREATE (:PT {r: $r})-[:PTR]->(:PTX)", map[string]interface{}{"r": payload})
			require.NoError(t, err)
			if explicit {
				_, err = executor.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}

			result, err := executor.Execute(ctx, "MATCH (source:PT)-[relationship:PTR]->(:PTX) RETURN source.r AS r, count(relationship) AS count", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{payload, int64(1)}}, result.Rows)

			result, err = executor.Execute(ctx, "MATCH ()-[relationship:X]->() RETURN count(relationship) AS count", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
		})
	}
}
