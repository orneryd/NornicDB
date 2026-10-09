package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Cypher 25's GROUP BY, with Neo4j 2026.09's answers.
func TestGroupByMatchesNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "group_by"))
	ctx := context.Background()
	run := func(query string) *ExecuteResult {
		result, err := exec.Execute(ctx, "CYPHER 25 "+query, nil)
		require.NoError(t, err, query)
		return result
	}
	run("CREATE (:S25 {name: 'Ann', age: 40}), (:S25 {name: 'Bob', age: 30}), (:S25 {name: 'Cy', age: 20})")
	run("CREATE (:G25 {name: 'Ann', age: 40}), (:G25 {name: 'Ann', age: 30}), (:G25 {name: 'Bob', age: 20})")
	l := func(values ...interface{}) []interface{} { return values }
	ordered := map[string][][]interface{}{
		"MATCH (n:S25) RETURN n.age > 25 AS old, count(*) AS c GROUP BY old ORDER BY old":                                      {l(false, int64(1)), l(true, int64(2))},
		"MATCH (n:S25) RETURN n.age > 25 AS old, count(*) AS c GROUP BY n.age > 25 ORDER BY old":                               {l(false, int64(1)), l(true, int64(2))},
		"MATCH (n:S25) RETURN n.name AS name GROUP BY name ORDER BY name":                                                      {l("Ann"), l("Bob"), l("Cy")},
		"MATCH (n:S25) RETURN count(*) AS c GROUP BY ()":                                                                       {l(int64(3))},
		"MATCH (n:S25) WITH n.age > 25 AS old, count(*) AS c GROUP BY old WHERE c > 1 RETURN old, c":                           {l(true, int64(2))},
		"WITH 1 AS group RETURN group":                                                                                         {l(int64(1))},
		"MATCH (n:G25) RETURN n.name AS name GROUP BY name ORDER BY name":                                                      {l("Ann"), l("Bob")},
		"MATCH (n:G25) RETURN n.name AS name GROUP BY name, n.age ORDER BY name":                                               {l("Ann"), l("Ann"), l("Bob")},
		"MATCH (n:G25) RETURN n.name, count(*) AS c GROUP BY n.name ORDER BY c":                                                {l("Bob", int64(1)), l("Ann", int64(2))},
		"MATCH (n:G25) RETURN n.name AS name, count(*) AS c GROUP BY n.name, name ORDER BY name":                               {l("Ann", int64(2)), l("Bob", int64(1))},
		"MATCH (n:G25) RETURN n.name AS name, count(*) + 1 AS c GROUP BY name ORDER BY name":                                   {l("Ann", int64(3)), l("Bob", int64(2))},
		"MATCH (n:G25) RETURN n.name + '!' AS name, count(*) AS c GROUP BY n.name ORDER BY name":                               {l("Ann!", int64(2)), l("Bob!", int64(1))},
		"MATCH (n:G25) RETURN n.name AS name, max(n.age) AS m GROUP BY name ORDER BY name SKIP 1":                              {l("Bob", int64(20))},
		"MATCH (n:G25) WITH n.name AS name, count(*) AS c GROUP BY name ORDER BY c DESC LIMIT 1 RETURN name, c":                {l("Ann", int64(2))},
		"MATCH (n:G25) RETURN count(*) AS c GROUP BY n.name, n.age ORDER BY c":                                                 {l(int64(1)), l(int64(1)), l(int64(1))},
		"MATCH (n:G25) RETURN size(collect(n.age)) AS ages GROUP BY n.name ORDER BY ages":                                      {l(int64(1)), l(int64(2))},
		"MATCH (n:G25) CALL (n) { RETURN n.name AS name, count(*) AS c GROUP BY name } RETURN name, sum(c) AS c ORDER BY name": {l("Ann", int64(2)), l("Bob", int64(1))},
	}
	for query, want := range ordered {
		require.Equal(t, want, run(query).Rows, query)
	}
	for query, want := range map[string][][]interface{}{
		"MATCH (n:S25) RETURN n.age > 25 AS old, count(*) AS c GROUP BY old, n.name":  {l(true, int64(1)), l(true, int64(1)), l(false, int64(1))},
		"MATCH (n:S25) RETURN count(*) AS c GROUP BY n.name":                          {l(int64(1)), l(int64(1)), l(int64(1))},
		"MATCH (n:S25) RETURN DISTINCT n.age > 25 AS old, count(*) AS c GROUP BY old": {l(true, int64(2)), l(false, int64(1))},
		"WITH 1 AS x RETURN x GROUP BY x UNION RETURN 2 AS x":                         {l(int64(1)), l(int64(2))},
	} {
		require.ElementsMatch(t, want, run(query).Rows, query)
	}
	require.Equal(t, []string{"n.name", "c"}, run("MATCH (n:G25) RETURN n.name, count(*) AS c GROUP BY n.name").Columns)

	for _, query := range []string{
		"MATCH (n:S25) RETURN n.age > 25 AS old, n.name AS name, count(*) AS c GROUP BY old",
		"MATCH (n:S25) RETURN n.name AS name, count(*) AS c GROUP BY nope",
	} {
		_, err := exec.Execute(ctx, "CYPHER 25 "+query, nil)
		require.Error(t, err, query)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
	_, err := exec.Execute(ctx, "CYPHER 25 MATCH (n:S25) RETURN n.age > 25 AS old, n.name AS name, count(*) AS c GROUP BY old", nil)
	require.ErrorContains(t, err, "Aggregation column contains implicit grouping expressions.")
}
