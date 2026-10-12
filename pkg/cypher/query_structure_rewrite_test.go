package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Cypher 25's NEXT, WHEN and braced query parts, with Neo4j 2026.09's
// answers.
func TestQueryStructureMatchesNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "query_structure"))
	ctx := context.Background()
	run := func(query string) *ExecuteResult {
		result, err := exec.Execute(ctx, "CYPHER 25 "+query, nil)
		require.NoError(t, err, query)
		return result
	}
	run("CREATE (:S25:M25 {name: 'Ann', age: 40}), (:S25 {name: 'Bob', age: 30}), (:S25 {name: 'Cy', age: 20})")
	l := func(values ...interface{}) []interface{} { return values }
	for query, want := range map[string][][]interface{}{
		"RETURN 1 AS a NEXT RETURN a + 1 AS b":                                            {l(int64(2))},
		"WITH 1 AS next RETURN next":                                                      {l(int64(1))},
		"CREATE (:X25) NEXT RETURN 1 AS a":                                                {l(int64(1))},
		"RETURN 1 AS a NEXT RETURN *":                                                     {l(int64(1))},
		"RETURN 1 AS a, 2 AS b NEXT RETURN b":                                             {l(int64(2))},
		"UNWIND [1, 2] AS x RETURN x ORDER BY x DESC LIMIT 1 NEXT RETURN x":               {l(int64(2))},
		"RETURN 1 AS a NEXT RETURN a NEXT RETURN a":                                       {l(int64(1))},
		"RETURN 1 AS a NEXT { RETURN a }":                                                 {l(int64(1))},
		"RETURN 1 AS a NEXT WHEN a = 1 THEN RETURN 'y' AS r ELSE RETURN 'n' AS r":         {l("y")},
		"MATCH (n:S25) RETURN n.name AS name NEXT RETURN size(collect(name)) AS c":        {l(int64(3))},
		"OPTIONAL MATCH (z:Nope) RETURN z NEXT RETURN z IS NULL AS v":                     {l(true)},
		"CALL () { RETURN 1 AS a NEXT RETURN a + 1 AS b } RETURN b":                       {l(int64(2))},
		"UNWIND [1, 2] AS x RETURN x NEXT UNWIND [10, 20] AS y RETURN x, y ORDER BY x, y": {l(int64(1), int64(10)), l(int64(1), int64(20)), l(int64(2), int64(10)), l(int64(2), int64(20))},
		"RETURN 1 AS a UNION RETURN 2 AS a NEXT RETURN sum(a) AS s":                       {l(int64(3))},
		"RETURN [1, 2] AS l NEXT UNWIND l AS x RETURN x":                                  {l(int64(1)), l(int64(2))},
		"WHEN false THEN RETURN 1 AS x WHEN true THEN RETURN 2 AS x ELSE RETURN 3 AS x":   {l(int64(2))},
		"WHEN false THEN RETURN 1 AS x":                                                   nil,
		"WHEN null THEN RETURN 1 AS x ELSE RETURN 2 AS x":                                 {l(int64(2))},
		"WHEN 1 = 1 THEN RETURN 'a' AS x WHEN 1 = 1 THEN RETURN 'b' AS x":                 {l("a")},
		"WHEN true THEN RETURN 1 AS x NEXT RETURN x + 1 AS y":                             {l(int64(2))},
		"WHEN true THEN { RETURN 1 AS x } ELSE { RETURN 2 AS x } NEXT RETURN x":           {l(int64(1))},
		"WHEN true THEN { MATCH (p:S25) RETURN count(p) AS x } ELSE { RETURN 0 AS x }":    {l(int64(3))},
		"WHEN EXISTS { MATCH (:M25) } THEN RETURN 'yes' AS x ELSE RETURN 'no' AS x":       {l("yes")},
		"UNWIND [1, 2] AS i CALL (i) { WHEN i = 1 THEN RETURN 'a' AS r } RETURN i, r":     {l(int64(1), "a")},
		"CALL () { WHEN true THEN RETURN 1 AS x } RETURN x":                               {l(int64(1))},
		"WITH 1 AS when RETURN when":                                                      {l(int64(1))},
		"MATCH (p:S25) CALL (p) { WHEN p.age > 35 THEN RETURN 'old' AS k ELSE RETURN 'young' AS k } RETURN p.name AS n, k ORDER BY n": {
			l("Ann", "old"), l("Bob", "young"), l("Cy", "young")},
		"RETURN CASE WHEN true THEN 1 ELSE 2 END AS v":                                              {l(int64(1))},
		"{ RETURN 1 AS x UNION RETURN 1 AS x } UNION ALL { RETURN 1 AS x UNION ALL RETURN 1 AS x }": {l(int64(1)), l(int64(1)), l(int64(1))},
		"{ RETURN 1 AS x }":                                             {l(int64(1))},
		"{ { RETURN 1 AS x } }":                                         {l(int64(1))},
		"{ RETURN 1 AS x } UNION RETURN 1 AS x":                         {l(int64(1))},
		"{ RETURN 1 AS x, 2 AS y } UNION { RETURN 3 AS x, 4 AS y }":     {l(int64(1), int64(2)), l(int64(3), int64(4))},
		"{ MATCH (n:S25) RETURN n.name AS v ORDER BY v LIMIT 1 }":       {l("Ann")},
		"{ RETURN 1 AS x } NEXT RETURN x":                               {l(int64(1))},
		"RETURN 1 AS x UNION ALL { RETURN 1 AS x UNION RETURN 1 AS x }": {l(int64(1)), l(int64(1))},
		"{ MATCH (n:S25) RETURN n.name AS name UNION MATCH (n:M25) RETURN n.name AS name } UNION ALL RETURN 'Z' AS name": {
			l("Ann"), l("Bob"), l("Cy"), l("Z")},
	} {
		result := run(query)
		if want == nil {
			require.Empty(t, result.Rows, query)
			continue
		}
		require.ElementsMatch(t, want, result.Rows, query)
	}
	require.Equal(t, []string{"a + 1"}, run("RETURN 1 AS a NEXT RETURN a + 1").Columns)
	require.Empty(t, run("RETURN 1 AS a NEXT FINISH").Rows)
	require.Empty(t, run("WHEN true THEN CREATE (:X25)").Rows)
	require.Empty(t, run("{ CREATE (:X25) }").Rows)

	// The conditions are evaluated once, before a branch writes.
	require.Equal(t, [][]interface{}{{"a"}}, run("WHEN NOT EXISTS { MATCH (:X26 {w: 1}) } THEN CREATE (:X26 {w: 1}) RETURN 'a' AS r WHEN EXISTS { MATCH (:X26 {w: 1}) } THEN RETURN 'b' AS r").Rows)
	require.Equal(t, [][]interface{}{{int64(1)}}, run("MATCH (x:X26) RETURN count(x) AS c").Rows)
	require.Equal(t, [][]interface{}{{int64(1)}}, run("UNWIND [1] AS w RETURN w").Rows, "a branch variable doesn't leak")

	for _, query := range []string{
		"WHEN true THEN RETURN 1 AS x ELSE RETURN 2 AS y",
		"WHEN true THEN RETURN 1 AS x WHEN false THEN RETURN 1 AS x, 2 AS y",
		"{ RETURN 1 AS x } UNION { RETURN 2 AS y }",
	} {
		_, err := exec.Execute(ctx, "CYPHER 25 "+query, nil)
		require.Error(t, err, query)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
}
