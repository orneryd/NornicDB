package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSearchedCaseInWhere covers #699: a searched CASE without parentheses in
// WHERE (compared, in arithmetic, or alone as a boolean) is one operand, so
// the comparisons, AND and OR of its WHEN conditions are not the predicate's.
// Results are Neo4j 5.26.30's.
func TestSearchedCaseInWhere(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	_, err := executor.Execute(ctx, "CREATE (:W {id: 'a', n: 3})-[:R {w: 2}]->(:W {id: 'b', n: 1}), (:W {id: 'c', n: 5})", nil)
	require.NoError(t, err)
	for query, want := range map[string][]interface{}{
		"MATCH (w:W) WHERE CASE WHEN w.n > 2 THEN w.n ELSE 0 END > 4 RETURN w.id AS id ORDER BY id":                            {"c"},
		"MATCH (w:W) WHERE 4 < CASE WHEN w.n > 2 THEN w.n ELSE 0 END RETURN w.id AS id ORDER BY id":                            {"c"},
		"MATCH (w:W) WHERE CASE WHEN w.n > 2 THEN w.n ELSE 0 END = 5 RETURN w.id AS id ORDER BY id":                            {"c"},
		"MATCH (w:W) WHERE w.n + CASE WHEN w.n > 2 THEN 1 ELSE 0 END > 4 RETURN w.id AS id ORDER BY id":                        {"c"},
		"MATCH (w:W) WITH w WHERE CASE WHEN w.n > 2 THEN w.n ELSE 0 END > 4 RETURN w.id AS id ORDER BY id":                     {"c"},
		"MATCH (w:W) WHERE CASE WHEN w.n > 2 THEN true ELSE false END RETURN w.id AS id ORDER BY id":                           {"a", "c"},
		"MATCH (w:W) WHERE NOT CASE WHEN w.n > 2 THEN true ELSE false END RETURN w.id AS id ORDER BY id":                       {"b"},
		"MATCH (w:W) WHERE CASE WHEN w.n > 2 AND w.n < 4 THEN 1 ELSE 0 END = 1 RETURN w.id AS id ORDER BY id":                  {"a"},
		"MATCH (w:W) WHERE CASE WHEN w.n > 2 OR w.id = 'b' THEN true ELSE false END AND w.n < 5 RETURN w.id AS id ORDER BY id": {"a", "b"},
		"MATCH (a:W)-[r:R]->(b:W) WHERE CASE WHEN r.w > 1 THEN b.n ELSE 0 END = 1 RETURN a.id AS id":                           {"a"},
		"UNWIND [1, 5] AS x WITH x WHERE CASE WHEN x > 2 THEN x ELSE 0 END > 4 RETURN x AS id":                                 {int64(5)},
	} {
		result, err := executor.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		got := make([]interface{}, 0, len(result.Rows))
		for _, row := range result.Rows {
			got = append(got, row[0])
		}
		require.Equal(t, want, got, query)
	}
}
