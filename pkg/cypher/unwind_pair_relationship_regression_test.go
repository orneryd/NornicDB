package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnwindPairsMatchEndpointsAndMergeRelationships(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	_, err := executor.Execute(ctx, "CREATE (:Item {id: 1}), (:Item {id: 2}), (:Item {id: 3}), (:Item {id: 4})", nil)
	require.NoError(t, err)

	result, err := executor.Execute(ctx, `
		UNWIND $pairs AS pair
		MATCH (source:Item {id: pair[0]}), (target:Item {id: pair[1]})
		MERGE (source)-[relationship:NEXT]->(target)
		SET relationship.weight = pair[0] * 10
		RETURN count(relationship) AS count
	`, map[string]interface{}{
		"pairs": []interface{}{
			[]interface{}{int64(3), int64(4)},
			[]interface{}{int64(1), int64(2)},
		},
	})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)

	stored, err := executor.Execute(ctx, "MATCH (source:Item)-[relationship:NEXT]->(target:Item) RETURN source.id, target.id, relationship.weight ORDER BY source.id", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), int64(2), int64(10)}, {int64(3), int64(4), int64(30)}}, stored.Rows)
}
