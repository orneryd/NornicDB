package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// RETURN / WITH ALL, interpolated strings, map comprehensions and IS
// [NOT] LABELED, with Neo4j 2026.09's answers.
func TestCypher25ExpressionsMatchNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "c25_expressions"))
	ctx := context.Background()
	run := func(query string) *ExecuteResult {
		result, err := exec.Execute(ctx, "CYPHER 25 "+query, nil)
		require.NoError(t, err, query)
		return result
	}
	run("CREATE (:S25:M25 {name: 'Ann', age: 40}), (:S25 {name: 'Bob', age: 30}), (:S25 {name: 'Cy', age: 20})")
	run("MATCH (a:S25 {name: 'Ann'}), (b:S25 {name: 'Bob'}) CREATE (a)-[:ACTED_IN]->(b)")
	l := func(values ...interface{}) []interface{} { return values }
	m := func(pairs ...interface{}) map[string]interface{} {
		out := map[string]interface{}{}
		for i := 0; i < len(pairs); i += 2 {
			out[pairs[i].(string)] = pairs[i+1]
		}
		return out
	}
	for query, want := range map[string][][]interface{}{
		"RETURN ALL 1 AS x":   {l(int64(1))},
		"RETURN ALL (1) AS x": {l(int64(1))},
		"WITH [1, 2] AS l RETURN ALL (x IN l WHERE x > 0) AS v":                      {l(true)},
		"WITH [1, 2] AS l RETURN ALL(x IN l WHERE x > 0) AS v":                       {l(true)},
		"WITH 1 AS all RETURN all":                                                   {l(int64(1))},
		"UNWIND [1, 1] AS x RETURN ALL x":                                            {l(int64(1)), l(int64(1))},
		"UNWIND [1, 1] AS x WITH ALL x RETURN collect(x) AS c":                       {l(l(int64(1), int64(1)))},
		"MATCH (n:S25) RETURN ALL n.age > 26 AS v ORDER BY v":                        {l(false), l(true), l(true)},
		"MATCH (n:S25) WHERE n.name STARTS WITH 'A' RETURN n.age AS v":               {l(int64(40))},
		`WITH 'A' AS a RETURN s"x{a}y" AS v`:                                         {l("xAy")},
		`RETURN s"a\{b" AS v`:                                                        {l("a{b")},
		`RETURN s"{1}{2}" AS v`:                                                      {l("12")},
		`RETURN s"{1.5}" AS v`:                                                       {l("1.5")},
		`RETURN s"{true}" AS v`:                                                      {l("true")},
		`RETURN s"{date('2020-01-01')}" AS v`:                                        {l("2020-01-01")},
		`RETURN s"{"q"}" AS v`:                                                       {l("q")},
		`RETURN s"" AS v`:                                                            {l("")},
		`RETURN S"x" AS v`:                                                           {l("x")},
		`WITH 1 AS s RETURN s AS v`:                                                  {l(int64(1))},
		`RETURN s"{null}" AS v`:                                                      {l(nil)},
		`RETURN s"a" + 'b' AS v`:                                                     {l("ab")},
		`RETURN s'{"a"}' AS v`:                                                       {l("a")},
		`RETURN s"{s"{1}"}" AS v`:                                                    {l("1")},
		`WITH 1 AS a, 2 AS b RETURN s"{a} + {b} = {a + b}" AS v`:                     {l("1 + 2 = 3")},
		`MATCH (p:S25 {name: 'Ann'}) RETURN s"{p.name} is {p.age}" AS v`:             {l("Ann is 40")},
		`WITH null AS x RETURN s"x={x}" AS v`:                                        {l(nil)},
		"RETURN {k: v IN {a: 1, b: 2} | k: v * 10} AS v":                             {l(m("a", int64(10), "b", int64(20)))},
		"RETURN {k: v IN {a: 1, b: 2} WHERE v > 1 | k: v} AS v":                      {l(m("b", int64(2)))},
		"RETURN {k: v IN {a: 1} | k + '_': v} AS v":                                  {l(m("a_", int64(1)))},
		"RETURN {k: v IN {a: 1, b: 2} | k: k} AS v":                                  {l(m("a", "a", "b", "b"))},
		"RETURN {k: v IN {a: {b: 1}} | k: {kk: vv IN v | kk: vv + 1}} AS v":          {l(m("a", m("b", int64(2))))},
		"RETURN [{k: v IN {a: 1} | k: v}] AS v":                                      {l(l(m("a", int64(1))))},
		"RETURN {k: v IN null | k: v} AS v":                                          {l(nil)},
		"RETURN {k: v IN {} | k: v} AS v":                                            {l(m())},
		"WITH {a: 1, b: 2} AS map RETURN {k: v IN map | toUpper(k): v} AS r":         {l(m("A", int64(1), "B", int64(2)))},
		"MATCH (n:S25) RETURN n.name AS n, n IS LABELED M25 AS v ORDER BY n":         {l("Ann", true), l("Bob", false), l("Cy", false)},
		"MATCH (n:S25) RETURN n.name AS n, n IS NOT LABELED M25 AS v ORDER BY n":     {l("Ann", false), l("Bob", true), l("Cy", true)},
		"MATCH (n:S25) RETURN n.name AS n, n IS LABELED M25 & S25 AS v ORDER BY n":   {l("Ann", true), l("Bob", false), l("Cy", false)},
		"MATCH (n:S25) RETURN n.name AS n, n IS LABELED !M25 AS v ORDER BY n":        {l("Ann", false), l("Bob", true), l("Cy", true)},
		"MATCH (n:S25) RETURN n.name AS n, n IS LABELED (M25 | X25) AS v ORDER BY n": {l("Ann", true), l("Bob", false), l("Cy", false)},
		"MATCH (n:S25) RETURN n.name AS n, n IS LABELED % AS v ORDER BY n":           {l("Ann", true), l("Bob", true), l("Cy", true)},
		"WITH null AS n RETURN n IS LABELED M25 AS v":                                {l(nil)},
		"MATCH (n:S25 {name: 'Ann'}) RETURN n IS LABELED M25 AND true AS v":          {l(true)},
		"MATCH (n:S25 {name: 'Ann'}) RETURN NOT n IS LABELED M25 AS v":               {l(false)},
		"MATCH (n:S25) WHERE n IS LABELED M25 RETURN n.name AS v":                    {l("Ann")},
		"MATCH (n:S25) WHERE n IS NOT LABELED M25 RETURN count(n) AS c":              {l(int64(2))},
		"MATCH ()-[r:ACTED_IN]->() RETURN r IS LABELED ACTED_IN AS v":                {l(true)},
	} {
		require.Equal(t, want, run(query).Rows, query)
	}
	require.Equal(t, []string{`s"{1 + 1}"`}, run(`RETURN s"{1 + 1}"`).Columns)
	require.Equal(t, []string{"x"}, run("UNWIND [1] AS x RETURN ALL x").Columns)

	for query, code := range map[string]string{
		`RETURN s"{[1, 2]}" AS v`:                        "Neo.ClientError.Statement.TypeError",
		`RETURN s"{ {a: 1} }" AS v`:                      "Neo.ClientError.Statement.TypeError",
		`MATCH (n:S25 {name: 'Ann'}) RETURN s"{n}" AS v`: "Neo.ClientError.Statement.TypeError",
		`RETURN s"{}" AS v`:                              "Neo.ClientError.Statement.SyntaxError",
		`RETURN s"}" AS v`:                               "Neo.ClientError.Statement.SyntaxError",
		`RETURN s"\{x}" AS v`:                            "Neo.ClientError.Statement.SyntaxError",
	} {
		_, err := exec.Execute(ctx, "CYPHER 25 "+query, nil)
		require.Error(t, err, query)
		requireStatusCode(t, err, code)
	}
	_, err := exec.Execute(ctx, `CYPHER 25 RETURN s"{[1, 2]}" AS v`, nil)
	require.ErrorContains(t, err, "Wrong type. Expected BOOLEAN, STRING, UUID, INTEGER, FLOAT, TEMPORAL, DURATION or VECTOR, got LIST<INTEGER NOT NULL> NOT NULL")
}

// A sign or a list right after RETURN ALL / WITH ALL, spaced or not, starts
// the projected expression in a Cypher 25 statement (Neo4j 2026.09); a
// Cypher 5 statement reads all + 1 as the variable all (Neo4j 5.26).
func TestCypher25ProjectionAllBeforeSignOrList(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "cypher25_projection_all"))
	ctx := context.Background()
	for _, tc := range []struct {
		query             string
		cypher25, cypher5 interface{}
	}{
		{"WITH 1 AS all RETURN all + 1 AS v", int64(1), int64(2)},
		{"WITH 1 AS all RETURN all+1 AS v", int64(1), int64(2)},
		{"WITH 1 AS all RETURN all - 1 AS v", int64(-1), int64(0)},
		{"WITH 2 AS all RETURN all - all AS v", int64(-2), int64(0)},
		{"WITH [5] AS all RETURN all [0] AS v", []interface{}{int64(0)}, int64(5)},
		{"WITH [5] AS all RETURN all[0] AS v", []interface{}{int64(0)}, int64(5)},
		{"WITH [5] AS all WITH all[0] AS v RETURN v", []interface{}{int64(0)}, int64(5)},
		{"WITH 1 AS all RETURN all * 2 AS v", int64(2), int64(2)},
		{"WITH 1 AS all RETURN all AS v", int64(1), int64(1)},
		{"WITH 2 AS all RETURN DISTINCT all - 1 AS v", int64(1), int64(1)},
	} {
		for prefix, want := range map[string]interface{}{"CYPHER 25 ": tc.cypher25, "": tc.cypher5} {
			result, err := exec.Execute(ctx, prefix+tc.query, nil)
			require.NoError(t, err, prefix+tc.query)
			require.Equal(t, [][]interface{}{{want}}, result.Rows, prefix+tc.query)
		}
	}
	result, err := exec.Execute(ctx, "CYPHER 25 WITH 1 AS all RETURN all + 1", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"+ 1"}, result.Columns)
}
