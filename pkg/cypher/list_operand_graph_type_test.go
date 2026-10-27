package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestListOperandGraphVariableIsTypeError: a node, relationship or path
// variable in a list position is Neo4j's compile-time SyntaxError "Type
// mismatch: expected List<T> but was Node", on every route and whatever the
// data. FOREACH over a node, UNWIND of a node, a comprehension variable that
// shadows a node and a variable-length relationship (a list) are not. Each
// statement's outcome is Neo4j 5.26.30's.
func TestListOperandGraphVariableIsTypeError(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "listop"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:QP {s: 5})-[:R]->(:QP {s: 6})", nil)
	require.NoError(t, err)

	for statement, typeName := range map[string]string{
		"MATCH (n:QP {s: 5}) RETURN 1 IN n AS r":                              "Node",
		"MATCH (n:QP {s: 5}) RETURN 1 IN n AS x, [n IN [1] | n] AS y":         "Node",
		"MATCH (n:QP {s: 5}) WITH n RETURN 1 IN n AS r":                       "Node",
		"CREATE (n:QP2) WITH n RETURN 1 IN n AS r":                            "Node",
		"MATCH ()-[r:R]->() RETURN 1 IN r AS x":                               "Relationship",
		"MATCH p = (:QP {s: 5})-->() RETURN 1 IN p AS x":                      "Path",
		"MATCH p = ()-->() RETURN [x IN p | 1] AS x":                          "Path",
		"MATCH (n:QP {s: 5}) RETURN [x IN n | x] AS x":                        "Node",
		"MATCH (n:QP {s: 5}) RETURN any(x IN n WHERE x = 1) AS x":             "Node",
		"MATCH (n:QP {s: 5}) RETURN reduce(a = 0, x IN n | a + 1) AS x":       "Node",
		"MATCH (n:QP {s: 5}) WITH n, 1 AS one RETURN one IN n AS x":           "Node",
		"MATCH (n:QP {s: 5}) WHERE 1 IN n RETURN n.s AS s":                    "Node",
		"MATCH (n) WITH n AS m WHERE 1 IN m RETURN m":                         "Node",
		"MATCH (n) WITH n WHERE 1 IN n RETURN n":                              "Node",
		"MATCH (n) WHERE EXISTS { MATCH (m) WHERE 1 IN n } RETURN n":          "Node",
		"MATCH (n) SET n.v = 1 IN n RETURN n":                                 "Node",
		"MATCH (n:QP {s: 5}) OPTIONAL MATCH (n)-[r:R]->() RETURN 2 IN r AS x": "Relationship",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.Error(t, err, statement)
		require.Contains(t, err.Error(), "Type mismatch: expected List<T> but was "+typeName, statement)
	}

	for statement, want := range map[string][][]interface{}{
		"MATCH (n:QP {s: 5}) FOREACH (x IN n | CREATE (:QF)) RETURN 1 AS one": {{int64(1)}},
		"MATCH (n:QP {s: 5}) UNWIND n AS x RETURN x.s AS s":                   {{int64(5)}},
		"MATCH (n:QP {s: 5}) RETURN [n IN [1, 2] | n] AS x":                   {{[]interface{}{int64(1), int64(2)}}},
		"MATCH (n:QP {s: 5}) RETURN [n /* gap */ IN [1, 2] | n] AS x":         {{[]interface{}{int64(1), int64(2)}}},
		"MATCH (n:QP {s: 5}) RETURN [n IN [[1], [2]] | 1 IN n] AS x":          {{[]interface{}{true, false}}},
		"MATCH (n:QP {s: 5}) RETURN any(n IN [[1]] WHERE 1 IN n) AS x":        {{true}},
		"MATCH (n:QP {s: 5}) RETURN n IN [n] AS x":                            {{true}},
		"MATCH (n:QP {s: 5})-[r*1..2]->() RETURN 1 IN r AS x":                 {{false}},
		"MATCH (n:QP {s: 5}) WITH n.s AS n RETURN 5 IN n AS x":                {{true}},
		"MATCH (n:QP {s: 5}) RETURN 5 IN n.s AS x":                            {{true}},
	} {
		result, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
		require.Equal(t, want, result.Rows, statement)
	}
}

func TestGraphListOperandIgnoresUnrelatedDeclarations(t *testing.T) {
	for _, header := range []string{"FOREACH (x IN n | SET x.ok = true)", "FOR x IN n RETURN x", "FOR /* header */ `x` IN n RETURN x"} {
		require.NoError(t, graphListOperandTypeError(header, matchSemanticScope{"n": matchBindingNode}), header)
	}
	for _, statement := range []string{"FOREACH (x IN [1] | SET x.ok = 1 IN n)", "FOR x IN [1] RETURN 1 IN n", "RETURN 'FOR x' AS note, 1 IN n", "RETURN 1 /* FOREACH (x */ IN n"} {
		require.ErrorContains(t, graphListOperandTypeError(statement, matchSemanticScope{"n": matchBindingNode}), "was Node", statement)
	}
	require.ErrorContains(t, graphListOperandTypeError("RETURN 1 IN n /* n IN [1] */ AS x", matchSemanticScope{"n": matchBindingNode}), "was Node")
	require.ErrorContains(t, graphListOperandTypeError("RETURN 1 IN n AS x, 'n IN [1]' AS note", matchSemanticScope{"n": matchBindingNode}), "was Node")
	require.ErrorContains(t, graphListOperandTypeError("RETURN 1 IN n AS x, [n IN [1] | n] AS y", matchSemanticScope{"n": matchBindingNode}), "was Node")
	require.ErrorContains(t, graphListOperandTypeError("RETURN [n IN [1] | n] AS y, 1 IN n AS x", matchSemanticScope{"n": matchBindingNode}), "was Node")
	require.ErrorContains(t, graphListOperandTypeError("RETURN [n IN n /* | ] */ | n] AS y", matchSemanticScope{"n": matchBindingNode}), "was Node")
	require.NoError(t, graphListOperandTypeError("RETURN [n IN [[1]] | '[' + (1 IN n)] AS y", matchSemanticScope{"n": matchBindingNode}))
}
