package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// allReduce and PROPERTY_EXISTS (Cypher 25), with Neo4j 2026.09's answers.
func TestAllReduceAndPropertyExists(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "all_reduce"))
	ctx := context.Background()
	rows := func(query string) [][]interface{} {
		result, err := exec.Execute(ctx, "CYPHER 25 "+query, nil)
		require.NoError(t, err, query)
		return result.Rows
	}
	rows("CREATE (n:PE {id: 1, s: 'a', l: [1]})-[:R {w: 1}]->(:PE {id: 2, s: 'b'}), (:PE {id: 3, s: null})")
	for query, want := range map[string]interface{}{
		"RETURN allReduce(acc = 0, x IN [1, 2, 3] | acc + x, acc < 10) AS v":                                    true,
		"RETURN allReduce(acc = 100, x IN [] | acc + x, acc < 3) AS v":                                          true,
		"RETURN allReduce(acc = 100, x IN [1] | x, acc < 3) AS v":                                               true,
		"RETURN allReduce(acc = 0, x IN [1, null, 3] | acc + x, acc < 10) AS v":                                 nil,
		"RETURN allReduce(acc = 0, x IN [1, 2] | x, CASE WHEN acc = 1 THEN null ELSE true END) AS v":            nil,
		"RETURN allReduce(acc = 0, x IN [1, 2, 3] | x, CASE WHEN acc = 1 THEN null ELSE acc < 3 END) AS v":      false,
		"RETURN allReduce(acc = 0, x IN null | x, true) AS v":                                                   nil,
		"RETURN allReduce(acc = 0, x IN [1,2] | acc + x, x > 0) AS v":                                           true,
		"RETURN allReduce(acc = 0, x IN [1,2] | acc + x, acc > 0 AND x > 1) AS v":                               false,
		"WITH [1, 2, 3] AS l RETURN allReduce(s = 0, x IN l | s + x, s <= 3) AS v":                              false,
		"RETURN ALLREDUCE(acc = 0, x IN [1] | acc + x, acc < 10) AS v":                                          true,
		"RETURN allReduce(acc = 0, x IN [1] | acc + x, acc > 0) AND true AS v":                                  true,
		"RETURN [allReduce(acc = 0, x IN [1] | acc + x, acc > 0)] AS v":                                         []interface{}{true},
		"MATCH p = (:PE)-->() RETURN allReduce(c = 0, r IN relationships(p) | c + r.w, c < 5) AS v":             true,
		"MATCH (n:PE {id: 1}) RETURN allReduce(c = '', k IN keys(n) | c + k, size(c) < 8) AS v":                 true,
		"RETURN reduce(acc = 0, x IN [1, 2, 3] | acc + x) AS v":                                                 int64(6),
		"MATCH (n:PE {id: 1}) RETURN PROPERTY_EXISTS(n, s) AS v":                                                true,
		"MATCH (n:PE {id: 1}) RETURN PROPERTY_EXISTS(n, nope) AS v":                                             false,
		"MATCH (n:PE {id: 1}) RETURN property_exists(n, `s`) AS v":                                              true,
		"MATCH (n:PE {id: 3}) RETURN PROPERTY_EXISTS(n, s) AS v":                                                false,
		"MATCH ()-[r:R]->() RETURN PROPERTY_EXISTS(r, w) AS v":                                                  true,
		"OPTIONAL MATCH (n:Nope) RETURN PROPERTY_EXISTS(n, s) AS v":                                             nil,
		"MATCH (n:PE) WHERE PROPERTY_EXISTS(n, l) RETURN count(n) AS v":                                         int64(1),
		"MATCH (n:PE {id: 1}) RETURN NOT PROPERTY_EXISTS(n, s) AS v":                                            false,
		"MATCH (n:PE) RETURN count(*) + sum(CASE WHEN PROPERTY_EXISTS(n, s) THEN 10 ELSE 0 END) AS v":           int64(23),
		"MATCH (n:PE) WITH n ORDER BY n.id RETURN collect(PROPERTY_EXISTS(n, s)) AS v":                          []interface{}{true, true, false},
		"MATCH (n:PE) WHERE allReduce(c = 0, x IN [n.id] | c + x, c < 2) RETURN collect(n.id) AS v":             []interface{}{int64(1)},
		"UNWIND [[1, 2], [5]] AS l WITH l WHERE allReduce(a = 0, x IN l | a + x, a < 4) RETURN collect(l) AS v": []interface{}{[]interface{}{int64(1), int64(2)}},
	} {
		got := rows(query)
		require.Len(t, got, 1, query)
		require.Equal(t, want, got[0][0], query)
	}
	result, err := exec.Execute(ctx, "CYPHER 25 WITH 5 AS acc RETURN allReduce(acc = 0, x IN [1] | acc + x, acc > 0), acc, PROPERTY_EXISTS(null, s)", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"allReduce(acc = 0, x IN [1] | acc + x, acc > 0)", "acc", "PROPERTY_EXISTS(null, s)"}, result.Columns)
	require.Equal(t, [][]interface{}{{true, int64(5), nil}}, result.Rows)

	for query, code := range map[string]string{
		"RETURN allReduce(acc = 0, x IN [1, 2] | x) AS v":             "Neo.ClientError.Statement.SyntaxError",
		"RETURN allReduce(acc = 0, x IN [1, 2] | x, true, 1) AS v":    "Neo.ClientError.Statement.SyntaxError",
		"RETURN allReduce(acc, x IN [1, 2] | x, true) AS v":           "Neo.ClientError.Statement.SyntaxError",
		"RETURN allReduce(acc = 0, x IN [1, 2] | x, 1) AS v":          "Neo.ClientError.Statement.SyntaxError",
		"RETURN allReduce(acc = 0, x IN 5 | x, true) AS v":            "Neo.ClientError.Statement.SyntaxError",
		"RETURN allReduce(acc = 0, x IN [1, 2] | acc + y, true) AS v": "Neo.ClientError.Statement.SyntaxError",
		"WITH 2 AS p RETURN allReduce(acc = 0, x IN [1] | x, p) AS v": "Neo.ClientError.Statement.SyntaxError",
		"WITH {s: 1} AS m RETURN PROPERTY_EXISTS(m, s) AS v":          "Neo.ClientError.Statement.SyntaxError",
		"WITH 1 AS m RETURN PROPERTY_EXISTS(m, s) AS v":               "Neo.ClientError.Statement.SyntaxError",
	} {
		_, err := exec.Execute(ctx, "CYPHER 25 "+query, nil)
		require.Error(t, err, query)
		requireStatusCode(t, err, code)
	}
	if !config.IsANTLRParser() {
		// The ANTLR parser rejects the call before the form check, as an
		// expression it can't read.
		_, err = exec.Execute(ctx, "CYPHER 25 RETURN allReduce(acc = 0, x IN [1, 2] | x) AS v", nil)
		require.ErrorContains(t, err, "Invalid syntax for the `allReduce` function. The function allReduce must have the signature allReduce(")
	}
}
