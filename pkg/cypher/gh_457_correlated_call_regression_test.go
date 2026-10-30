package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH457_CorrelatedCallPreservesOuterCompany(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh457")
			for _, node := range []*storage.Node{
				{ID: "acme", Labels: []string{"Company"}, Properties: map[string]interface{}{"name": "Acme"}},
				{ID: "bolt", Labels: []string{"Company"}, Properties: map[string]interface{}{"name": "Bolt"}},
				{ID: "ada", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Ada"}},
				{ID: "lin", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Lin"}},
				{ID: "max", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Max"}},
			} {
				_, err := store.CreateNode(node)
				require.NoError(t, err)
			}
			for _, edge := range []*storage.Edge{
				{ID: "ada-acme", StartNode: "ada", EndNode: "acme", Type: "WORKS_AT"},
				{ID: "lin-acme", StartNode: "lin", EndNode: "acme", Type: "WORKS_AT"},
				{ID: "max-bolt", StartNode: "max", EndNode: "bolt", Type: "WORKS_AT"},
			} {
				require.NoError(t, store.CreateEdge(edge))
			}
			exec := NewStorageExecutor(store)
			ctx := context.Background()
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}

			result, err := exec.Execute(ctx, `
MATCH (c:Company)
CALL {
  WITH c
  MATCH (p:Person)-[:WORKS_AT]->(c)
  RETURN count(p) AS n
}
RETURN c.name AS company, n ORDER BY company`, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"Acme", int64(2)}, {"Bolt", int64(1)}}, result.Rows)

			if explicit {
				_, err := exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		})
	}
}
