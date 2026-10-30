package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestProjectedExpressionsKeepTheirStaticType: a variable a WITH, UNWIND or
// CALL subquery binds has the static type Neo4j gives its expression, and the
// pattern, function and operator checks read that one type (#1055). Each
// statement's outcome is Neo4j 5.26.30's.
func TestProjectedExpressionsKeepTheirStaticType(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "statictypes"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Q {id: 1})-[:R {w: 2}]->(:Q {id: 2})", nil)
	require.NoError(t, err)
	const match = "MATCH p = (n:Q {id: 1})-[r:R]->(o:Q) "

	for statement, code := range map[string]string{
		// A property holds a property value: never a node or relationship.
		"WITH n.id AS v RETURN labels(v) AS x":                     "SyntaxError",
		"UNWIND [n.id] AS v RETURN type(v) AS x":                   "SyntaxError",
		"WITH [n.id] AS l UNWIND l AS v MATCH (v) RETURN 1":        "SyntaxError",
		"UNWIND [r.w] AS v MATCH ()-[v]->() RETURN count(*)":       "SyntaxError",
		"WITH collect(n.id) AS l UNWIND l AS v MATCH (v) RETURN 1": "SyntaxError",
		// An element of a list of nodes is a node.
		"WITH [n, o][0] AS v MATCH ()-[v]->() RETURN count(*) AS c":  "SyntaxError",
		"WITH [n, o][0] AS v RETURN toUpper(v) AS x":                 "SyntaxError",
		"WITH [n, o][0] AS v RETURN v + 1 AS x":                      "SyntaxError",
		"WITH [n, o][0] AS v RETURN 1 IN v AS x":                     "SyntaxError",
		"UNWIND [[n, o][0]] AS v RETURN type(v) AS x":                "SyntaxError",
		"WITH nodes(p)[0] AS v MATCH ()-[v]->() RETURN count(*)":     "SyntaxError",
		"WITH head(relationships(p)) AS v MATCH (v) RETURN 1":        "SyntaxError",
		"WITH startNode(r) AS v RETURN type(v) AS x":                 "SyntaxError",
		"WITH [x IN [n] | x] AS l UNWIND l AS v RETURN type(v) AS x": "SyntaxError",
		"WITH coalesce(n, o) AS v RETURN type(v) AS x":               "SyntaxError",
		// Whether coalesce(n, null) is a node is known when the row runs.
		"WITH coalesce(n, null) AS v RETURN length(v) AS x": "TypeError",
		// A map in a list may be a node or relationship: MATCH reads it
		// when the row runs.
		"UNWIND [n{.id}] AS v MATCH (v) RETURN count(*) AS c":                  "TypeError",
		"WITH [n{.id}] AS l UNWIND l AS v MATCH (v)-[:R]->(x) RETURN count(x)": "TypeError",
	} {
		_, err := exec.Execute(ctx, match+statement, nil)
		require.Error(t, err, statement)
		require.True(t, strings.HasPrefix(statusText(err), "Neo.ClientError.Statement."+code+":"), "%s: %s", statement, statusText(err))
	}

	// Neo4j names a property value's type, and a list of them, choice by
	// choice.
	requireStatus(t, exec, match+"WITH n.id AS v RETURN labels(v) AS x", nil, "Neo.ClientError.Statement.SyntaxError",
		"Type mismatch: expected Node but was "+staticPropertyValueType)
	requireStatus(t, exec, match+"WITH collect(n.id) AS v RETURN labels(v) AS x", nil, "Neo.ClientError.Statement.SyntaxError",
		"Type mismatch: expected Node but was "+listOfType(staticPropertyValueType))

	for statement, want := range map[string][][]interface{}{
		// A property value may be any of the property types: an operator
		// reads it as a value of unknown type.
		"WITH n.id AS v RETURN v + 1 AS x":                                            {{int64(2)}},
		"WITH n.id AS v RETURN 1 IN v AS x":                                           {{true}},
		"UNWIND [n.id] AS v RETURN v * 2 AS x":                                        {{int64(2)}},
		"WITH collect(n.id) AS l RETURN l[0] + 1 AS x":                                {{int64(2)}},
		"WITH coalesce(n, null) AS v MATCH (v)-[:R]->(x) RETURN x.id AS id":           {{int64(2)}},
		"WITH [n, o][0] AS v MATCH (v)-[:R]->(x) RETURN x.id AS id":                   {{int64(2)}},
		"WITH coalesce(1, 2) AS v RETURN v + 1 AS x":                                  {{int64(2)}},
		"WITH nodes(p)[0..1] AS l UNWIND l AS v RETURN v.id AS id":                    {{int64(1)}},
		"CALL { MATCH (m:Q {id: 2}) RETURN m } MATCH (m)<-[:R]-(x) RETURN x.id AS id": {{int64(1)}},
	} {
		result, err := exec.Execute(ctx, match+statement, nil)
		require.NoError(t, err, statement)
		require.Equal(t, want, result.Rows, statement)
	}
}

// listOfType and elementOfType map a type to its list type and back, choice
// by choice.
func TestListAndElementTypes(t *testing.T) {
	require.Equal(t, "List<Node>", listOfType("Node"))
	require.Equal(t, "List<Map>, List<Node> or List<Relationship>", listOfType("Map, Node or Relationship"))
	require.Equal(t, "Map, Node or Relationship", elementOfType("List<Map>, List<Node> or List<Relationship>"))
	require.Equal(t, staticPropertyValueType, elementOfType(listOfType(staticPropertyValueType)))
	require.Equal(t, "", elementOfType("List<T>"))
	require.Equal(t, "", elementOfType("Integer"))
	require.Equal(t, "", elementOfType(""))
	require.Equal(t, matchBindingUnknown, knownOperand("Map, Node or Relationship").bindingKind())
	require.Equal(t, matchBindingValue, typedOperand(staticPropertyValueType).bindingKind())
}

// An operator inside a CASE that is only part of an expression doesn't type
// the whole expression: [{a: 1}] + CASE WHEN x OR y = z THEN [] … END is a
// list, so the variable UNWIND binds from it is its element. Each
// statement's outcome is Neo4j 5.26.30's.
func TestCaseInsideAnExpressionDoesNotTypeIt(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "caseoperand"))
	ctx := context.Background()
	for statement, want := range map[string][][]interface{}{
		"WITH [{a: 1}] + CASE WHEN 1 = 1 THEN [] ELSE [{a: 2}] END AS l UNWIND l AS x RETURN x.a AS v":  {{int64(1)}},
		"WITH [1] + CASE WHEN 1 = 1 OR false THEN [] ELSE [2] END AS l UNWIND l AS x RETURN x + 1 AS v": {{int64(2)}},
		"WITH 2 AS i RETURN CASE WHEN i > 1 THEN 'a' ELSE 'b' END + 'x' AS v":                           {{"ax"}},
		"OPTIONAL MATCH (o:Nope) WITH [{node: 1, matched: true}] + CASE WHEN o IS NULL OR o.id = 1 THEN [] ELSE [{node: o, matched: false}] END AS entries UNWIND entries AS entry RETURN entry.node AS n, max(CASE WHEN entry.matched THEN 1 ELSE 0 END) = 1 AS m": {{int64(1), true}},
	} {
		result, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
		require.Equal(t, want, result.Rows, statement)
	}
	require.Equal(t, "[1] + CASE"+strings.Repeat(" ", len(" WHEN a OR b THEN [] "))+"END", maskCaseBlocks(originalBraceView("[1] + CASE WHEN a OR b THEN [] END")))
	require.Equal(t, "n.case = 'CASE x END'", maskCaseBlocks(expressionBraceView("n.case = 'CASE x END'")))
}
