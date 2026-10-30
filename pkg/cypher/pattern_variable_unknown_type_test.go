package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A variable whose type isn't known before the statement runs (a map's member,
// an element of a list of maps) may be used in a later pattern; Neo4j checks
// the value when the row runs. A type known to be wrong is rejected before
// the statement runs. Recorded on Neo4j 5.26.30.
func TestPatternVariableOfUnknownType(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "pattern_unknown_type"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:ZL {id:'a'})-[:R]->(:ZM {id:'b'})", nil)
	require.NoError(t, err)
	for query, want := range map[string][][]interface{}{
		"MATCH (n:ZL) WITH collect({node: n}) AS xs UNWIND xs AS x WITH x.node AS m MATCH (m)-[:R]->(o) RETURN m.id AS id, o.id AS other":                         {{"a", "b"}},
		"MATCH (n:ZL) WITH collect(n) AS xs UNWIND range(0, size(xs) - 1) AS i WITH xs[i] AS m MATCH (m)-[:R]->(o) RETURN m.id AS id, o.id AS other":              {{"a", "b"}},
		"MATCH (node:ZL) WITH collect({node: node}) AS xs UNWIND xs AS x WITH x.node AS node OPTIONAL MATCH (node)-[:R]->(o) RETURN node.id AS id, o.id AS other": {{"a", "b"}},
		"MATCH (n:ZL) WITH {node: n} AS x WITH x.node AS m MATCH (m) RETURN m.id AS id":                                                                           {{"a"}},
		"MATCH (n:ZL)-[r:R]->() WITH [r] AS rs WITH rs[0] AS m MATCH ()-[m]->(o) RETURN o.id":                                                                     {{"b"}},
		"MATCH (:ZL)-[r:R]->() UNWIND [r] AS m MATCH ()-[m]->(o) RETURN o.id":                                                                                     {{"b"}},
		"MATCH (n:ZL) WITH (n) AS m MATCH (m)-->(o) RETURN o.id":                                                                                                  {{"b"}},
		"MATCH (n:ZL) UNWIND [n] AS m MATCH (m)-[:R]->(o) RETURN o.id":                                                                                            {{"b"}},
		"CYPHER 25 MATCH (n:ZL) LET m = [n][0] MATCH (m)-[:R]->(o) RETURN o.id":                                                                                   {{"b"}},
		"MATCH (n:ZL) WITH {e: n} AS x WITH x.e AS m MATCH ()-[m]->(o) RETURN o.id":                                                                               {},
		"WITH {node: null} AS x WITH x.node AS m MATCH (m) RETURN m":                                                                                              {},
		"WITH {node: null} AS x WITH x.node AS m OPTIONAL MATCH (m)-[:R]->(o) RETURN m, o":                                                                        {{nil, nil}},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		if len(want) == 0 {
			require.Empty(t, result.Rows, query)
			continue
		}
		require.Equal(t, want, result.Rows, query)
	}
	for query, code := range map[string]string{
		"WITH 'a' AS m MATCH (m)-[:R]->(o) RETURN o.id":                                     "SyntaxError",
		"UNWIND [1] AS m MATCH (m)-[:R]->(o) RETURN o.id":                                   "SyntaxError",
		"MATCH (n:ZL)-[r:R]->() WITH [r] AS rs WITH rs[0] AS m MATCH (m)-->(o) RETURN o.id": "SyntaxError",
		"MATCH (n:ZL) WITH [n] AS ns WITH ns[0] AS m MATCH ()-[m]->(o) RETURN o.id":         "SyntaxError",
		"MATCH (n:ZL) WITH collect(n) AS ns WITH ns[0] AS m MATCH ()-[m]->(o) RETURN o.id":  "SyntaxError",
		"MATCH (n:ZL) WITH n.id AS m MATCH (m) RETURN m":                                    "SyntaxError",
		"MATCH (n:ZL) WITH toUpper(n.id) AS m MATCH (m) RETURN m":                           "SyntaxError",
		"WITH 1 + 2 AS m MATCH (m) RETURN m":                                                "SyntaxError",
		"MATCH (:ZL)-[r:R]->() UNWIND [r] AS m MATCH (m) RETURN m":                          "SyntaxError",
		"MATCH (n:ZL) WITH count(n) AS m MATCH (m) RETURN m":                                "SyntaxError",
		"WITH {node: 'a'} AS x WITH x.node AS m MATCH (m) RETURN m":                         "TypeError",
		"WITH {node: 'a'} AS x WITH x.node AS m MATCH (m:ZL) RETURN m":                      "TypeError",
		"WITH {node: 'a'} AS x WITH x.node AS m MATCH (m {id: 'a'}) RETURN m":               "TypeError",
		"WITH {node: 'a'} AS x WITH x.node AS m MATCH (m)-[:R]->(o) RETURN o":               "TypeError",
		"WITH {node: 'a'} AS x WITH x.node AS m OPTIONAL MATCH (m)-[:R]->(o) RETURN o":      "TypeError",
		"MATCH (n:ZL)-[r:R]->() WITH {e: r} AS x WITH x.e AS m MATCH (m)-->(o) RETURN o.id": "TypeError",
		"WITH [1, 'a'] AS l UNWIND l AS m MATCH (m) RETURN m":                               "TypeError",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		requireStatusCode(t, err, "Neo.ClientError.Statement."+code)
	}
}
