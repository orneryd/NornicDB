package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestRetrievePageColumnDeclared: a paged db.retrieve / db.rretrieve returns
// page, which the signature declares, so YIELD page works after MATCH / WITH
// as it does standalone, and SHOW PROCEDURES lists it (#946).
func TestRetrievePageColumnDeclared(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:PDGraphNode {id: 'a'})", nil)
	require.NoError(t, err)
	params := map[string]any{"request": map[string]any{"query": "source", "embedding": []float64{1, 0, 0}, "mode": "ranked", "n": 1, "limit": 2}}
	for _, procedure := range []string{"db.retrieve", "db.rretrieve"} {
		for _, query := range []string{
			"CALL " + procedure + "($request) YIELD page RETURN page IS NOT NULL AS ok",
			"MATCH (d:PDGraphNode {id: 'a'}) WITH d CALL " + procedure + "($request) YIELD page RETURN page IS NOT NULL AS ok",
		} {
			result, err := exec.Execute(ctx, query, params)
			require.NoError(t, err, query)
			require.Equal(t, [][]interface{}{{true}}, result.Rows, query)
		}
		result, err := exec.Execute(ctx, "SHOW PROCEDURES YIELD name, signature WHERE name = $name RETURN signature", map[string]any{"name": procedure})
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		require.True(t, strings.HasSuffix(result.Rows[0][0].(string), ", page :: MAP)"), result.Rows[0][0])
	}
}
