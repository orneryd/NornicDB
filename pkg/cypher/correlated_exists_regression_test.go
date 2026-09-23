package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExistsSubqueryCorrelatesBoundEndpoint(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	_, err := executor.Execute(ctx, `
		CREATE (:Order {id: 1}),
		       (:Item {sku: 'a1'}), (:Item {sku: 'b2'}),
		       (:Item {sku: 'c3'}), (:Item {sku: 'd4'})
	`, nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, `
		MATCH (order:Order {id: 1}), (item:Item)
		WHERE item.sku IN ['a1', 'c3']
		CREATE (order)-[:HAS]->(item)
	`, nil)
	require.NoError(t, err)

	result, err := executor.Execute(ctx, `
		MATCH (item:Item)
		WHERE EXISTS { (:Order)-[:HAS]->(item) }
		RETURN item.sku AS sku ORDER BY sku
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a1"}, {"c3"}}, result.Rows)

	result, err = executor.Execute(ctx, `
		MATCH (item:Item)
		WHERE NOT EXISTS { (:Order)-[:HAS]->(item) }
		RETURN item.sku AS sku ORDER BY sku
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"b2"}, {"d4"}}, result.Rows)
}
