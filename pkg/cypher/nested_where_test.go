package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A WHERE inside a list comprehension, a list predicate or braces belongs to
// them, not to the clause or the predicate around it: one keyword scanner
// skips lists and braces for every caller.
func TestNestedWhereBelongsToItsExpression(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nested_where"))
	ctx := context.Background()
	for query, want := range map[string][][]interface{}{
		// The WITH's projection keeps z.
		"WITH [1, 2] AS xs WITH [x IN xs WHERE x = 1] AS z CALL (z) { UNWIND z AS q RETURN collect(q) AS qs } RETURN qs": {{[]interface{}{int64(1)}}},
		"WITH [1, 2] AS xs WITH any(x IN xs WHERE x = 2) AS z CALL (z) { RETURN z AS w } RETURN w":                       {{true}},
		"WITH [1, 2] AS xs WITH [x IN xs WHERE x > 1] AS z WHERE size(z) = 1 RETURN z":                                   {{[]interface{}{int64(2)}}},
		// The list predicate's own WHERE.
		"RETURN any(x IN [y IN ['a', '', 'b'] WHERE y <> ''] WHERE x = 'b') AS r":             {{true}},
		"WITH 'b' AS t RETURN any(x IN [y IN ['a', '', 'b'] WHERE y <> ''] WHERE x = t) AS r": {{true}},
		"RETURN all(x IN [y IN [1, 2, 3] WHERE y > 1] WHERE x > 1) AS r":                      {{true}},
		"RETURN none(x IN [y IN [1, 2, 3] WHERE y > 1 | y * 10] WHERE x < 20) AS r":           {{true}},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}
}

// A subquery inside a CASE in WHERE runs for the row, with the row's
// variables: NOT CASE … THEN EXISTS { … } END keeps
// the row whose subquery finds nothing.
func TestCaseSubqueryInWhereSeesTheRow(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "case_subquery"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Plain)-[:RELATION {id: 'r1', kind: 'merged_into'}]->(:Plain)", nil)
	require.NoError(t, err)
	query := `WITH $id AS element, 'link' AS name
WITH element, name
WHERE NOT CASE name WHEN 'link'
          THEN EXISTS { MATCH ()-[n:RELATION {id: element}]->() WHERE n.kind = 'merged_into' } END
RETURN count(*) AS c`
	for id, want := range map[string]int64{"r1": 0, "missing": 1} {
		result, err := exec.Execute(ctx, query, map[string]interface{}{"id": id})
		require.NoError(t, err, id)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, id)
		positive, err := exec.Execute(ctx, "WITH $id AS element WITH element WHERE CASE true WHEN true THEN EXISTS { MATCH ()-[n:RELATION {id: element}]->() } END RETURN count(*) AS c", map[string]interface{}{"id": id})
		require.NoError(t, err, id)
		require.Equal(t, [][]interface{}{{1 - want}}, positive.Rows, id)
	}
}
