package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreatePropertyCanReferenceEarlierNodeInSameClause(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
		CREATE (a:End {num: 42, id: 0}),
		       (:End {num: 3}),
		       (:Begin {num: a.id})
	`, nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
		MATCH (a:Begin)
		WITH a.num AS property
		MATCH (b)
		WHERE b.id = property
		RETURN b
	`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	node := result.Rows[0][0]
	require.Equal(t, int64(42), extractPropertyFromValue(node, "num"))
	require.Equal(t, int64(0), extractPropertyFromValue(node, "id"))
}

func TestPipelineReturnDistinctUsesProjectedColumnNameAndValues(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	require.NoError(t, seedNodes(exec, ctx, []map[string]interface{}{
		{"id": int64(1)},
		{"id": int64(1)},
		{"id": int64(2)},
	}))

	result, err := exec.Execute(ctx, "MATCH (node) WITH node.id AS id RETURN DISTINCT id", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"id"}, result.Columns)
	require.ElementsMatch(t, [][]interface{}{{int64(1)}, {int64(2)}}, result.Rows)
}

func seedNodes(exec *StorageExecutor, ctx context.Context, properties []map[string]interface{}) error {
	for _, propertyMap := range properties {
		_, err := exec.Execute(ctx, "CREATE (:Value {id: $id})", propertyMap)
		if err != nil {
			return err
		}
	}
	return nil
}
