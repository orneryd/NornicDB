package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Since Cypher 25 (Neo4j 2026.09) any clause may follow a SHOW command's
// YIELD, and the query continues over the yielded columns; RETURN and those
// clauses see only them. The answers are Neo4j's (#907).
func TestShowCompositionMatchesNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "show_compose"))
	ctx := context.Background()
	l := func(values ...interface{}) []interface{} { return values }
	for query, want := range map[string][][]interface{}{
		"SHOW FUNCTIONS YIELD name, signature WITH name, collect(signature) AS s FILTER size(s) > 100 RETURN count(name) AS c": {l(int64(0))},
		"SHOW FUNCTIONS YIELD name WITH name WHERE name = 'abs' RETURN name":                                                   {l("abs")},
		"SHOW FUNCTIONS YIELD name WITH name WHERE name STARTS WITH 'ab' RETURN name ORDER BY name LIMIT 2":                    {l("abs")},
		"SHOW FUNCTIONS YIELD name UNWIND [1, 2] AS i WITH name, i WHERE name = 'abs' RETURN name, i":                          {l("abs", int64(1)), l("abs", int64(2))},
		"SHOW FUNCTIONS YIELD name MATCH (n) WHERE false RETURN name":                                                          {},
		"SHOW FUNCTIONS YIELD name WHERE name = 'abs' WITH name RETURN name":                                                   {l("abs")},
		"SHOW FUNCTIONS YIELD name ORDER BY name LIMIT 2 WITH name RETURN collect(name) AS c":                                  {l(l("abs", "acos"))},
		"SHOW FUNCTIONS YIELD name FILTER name = 'abs' RETURN name":                                                            {l("abs")},
		"SHOW FUNCTIONS YIELD name CALL (name) { RETURN name AS m } WITH m WHERE m = 'abs' RETURN m":                           {l("abs")},
		"SHOW PROCEDURES YIELD name WITH name WHERE name = 'db.labels' RETURN name":                                            {l("db.labels")},
		"SHOW INDEXES YIELD name WITH count(*) AS c RETURN c >= 0 AS ok":                                                       {l(true)},
		"SHOW FUNCTIONS YIELD name WITH name WHERE name = 'abs' RETURN name UNION RETURN 'x' AS name":                          {l("abs"), l("x")},
		"SHOW FUNCTIONS YIELD name AS fn WHERE name = 'abs' RETURN fn":                                                         {l("abs")},
		"SHOW FUNCTIONS YIELD name AS fn ORDER BY name LIMIT 1 RETURN fn":                                                      {l("abs")},
		"CALL db.labels() YIELD label FILTER label = 'Nope' RETURN label":                                                      {},
	} {
		result, err := exec.Execute(ctx, "CYPHER 25 "+query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}

	_, err := exec.Execute(ctx, "CYPHER 25 SHOW FUNCTIONS YIELD name WITH name WHERE name = 'abs' CREATE (:ShowX) RETURN name", nil)
	require.NoError(t, err)
	count, err := exec.Execute(ctx, "MATCH (n:ShowX) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, count.Rows, "a write after YIELD runs")

	for query, name := range map[string]string{
		"SHOW FUNCTIONS YIELD name WITH category RETURN category": "category",
		"SHOW FUNCTIONS YIELD name RETURN category":               "category",
		"SHOW FUNCTIONS YIELD name AS fn RETURN name":             "name",
	} {
		_, err := exec.Execute(ctx, "CYPHER 25 "+query, nil)
		require.ErrorContains(t, err, "variable "+name+" is not defined", query)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
	for _, query := range []string{
		"SHOW FUNCTIONS YIELD * WITH name WHERE name = 'abs' RETURN name",
		"SHOW FUNCTIONS WITH 1 AS x RETURN x",
	} {
		_, err := exec.Execute(ctx, "CYPHER 25 "+query, nil)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
}
