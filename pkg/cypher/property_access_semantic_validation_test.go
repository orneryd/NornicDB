package cypher

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPropertyAccessRejectsStaticallyNonPropertyValues(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	for _, expression := range []string{"123", "42.45", "true", "false", "'string'", "[123, true]"} {
		t.Run(expression, func(t *testing.T) {
			_, err := exec.Execute(ctx, "WITH "+expression+" AS value RETURN value.num", nil)
			require.Error(t, err)
			var semanticError *SemanticError
			require.True(t, errors.As(err, &semanticError))
			require.Equal(t, "InvalidArgumentType", semanticError.Detail)
		})
	}
}

func TestPropertyAccessRetainsMapAndNullSemantics(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	result, err := exec.Execute(ctx, "WITH {num: 7} AS value RETURN value.num", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)

	result, err = exec.Execute(ctx, "WITH null AS value RETURN value.num", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{nil}}, result.Rows)
}

func TestPropertyAccessSupportsDelimitedMapKeys(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	result, err := exec.Execute(ctx, "WITH {name: 'Mats', `a.b`: 'dot', `back``tick`: 'escaped'} AS value RETURN value.`name`, value.`a.b`, value.`back``tick`", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"Mats", "dot", "escaped"}}, result.Rows)
}

// TestPropertyAccessTypeMismatchMatchesNeo4j: a property access on a base of
// a static type without properties (a literal, a parameter, a variable bound
// to one) is Neo4j's SyntaxError wherever it is; a stored value is checked at
// run time with Neo4j's TypeError; a Float parameter only at run time. Every
// expectation is Neo4j 5.26's answer.
func TestPropertyAccessTypeMismatchMatchesNeo4j(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:PA {s: 'x', l: ['p', 'q'], v: 5})", nil)
	require.NoError(t, err)
	const expected = "Type mismatch: expected Map, Node, Relationship, Point, Duration, Date, Time, LocalTime, LocalDateTime or DateTime but was "
	for query, message := range map[string]string{
		"RETURN 'x'.y AS y":                                            expected + "String",
		"WITH 1.5 AS s RETURN s.x AS y":                                expected + "Float",
		"WITH ['p'] AS s RETURN s.x AS y":                              expected + "List<String>",
		"WITH [1, null] AS s RETURN s.x AS y":                          expected + "List<T>",
		"WITH [1, 2.5] AS s RETURN s.x AS y":                           expected + "List<Float>, List<Integer> or List<Number>",
		"UNWIND [1] AS i RETURN i.a AS y":                              expected + "Integer",
		"WITH 5 AS s RETURN [x IN [1] | s.a] AS r":                     expected + "Integer",
		"WITH 5 AS s RETURN toString(s.a) AS r":                        expected + "Integer",
		"WITH 5 AS s RETURN s.a + 1 AS r":                              expected + "Integer",
		"WITH 5 AS s ORDER BY s.a RETURN s":                            expected + "Integer",
		"WITH 5 AS s WHERE s.x = 1 RETURN s":                           expected + "Integer",
		"WITH 5 AS s, {a: 1} AS m RETURN m.a, s.b":                     expected + "Integer",
		"WITH 'x' AS s RETURN CASE WHEN true THEN s.a END AS r":        expected + "String",
		"WITH 5 AS s MATCH (n {k: s.a}) RETURN n":                      expected + "Integer",
		"WITH 5 AS s RETURN reduce(t = 0, x IN [1] | t + s.a) AS r":    expected + "Integer",
		"WITH 5 AS s MATCH (n:PA) RETURN [(n)-->(m) | s.a] AS r":       expected + "Integer",
		"WITH 5 AS s RETURN exists { MATCH (n) WHERE n.v = s.a } AS r": expected + "Integer",
		"WITH 5 AS s RETURN COLLECT { MATCH (n) RETURN s.a } AS r":     expected + "Integer",
		"WITH 5 AS s RETURN s {.a} AS r":                               "Type mismatch: expected Map, Node or Relationship but was Integer",
		"WITH 5 AS s RETURN s {k: 1} AS r":                             "Type mismatch: expected Map, Node or Relationship but was Integer",
		"MATCH (n:PA) WITH n.s AS s RETURN s.x AS y":                   "Type mismatch: expected a map but was String(\"x\")",
		"MATCH (n:PA) WITH n.l AS s RETURN s.x AS y":                   "Type mismatch: expected a map but was StringArray[p, q]",
		"MATCH (n:PA) WITH n.v AS s RETURN s.x AS y":                   "Type mismatch: expected a map but was Long(5)",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.ErrorContains(t, err, message, query)
	}
	for query, rows := range map[string][][]interface{}{
		"WITH 5 AS s RETURN [s IN [{a: 1}] | s.a] AS r":            {{[]interface{}{int64(1)}}},
		"WITH 5 AS s RETURN any(s IN [{a: 1}] WHERE s.a = 1) AS r": {{true}},
		"WITH 5 AS db RETURN db AS y":                              {{int64(5)}},
		"WITH {a: 1} AS s RETURN s {.a} AS r":                      {{map[string]interface{}{"a": int64(1)}}},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, rows, result.Rows, query)
	}

	for _, tc := range []struct {
		value    interface{}
		typeName string
	}{
		{int64(5), "Integer"}, {"str", "String"}, {true, "Boolean"},
		{[]interface{}{"p", "q"}, "List<String>"}, {[]string{"a"}, "List<String>"},
		{[]interface{}{int64(1), "a"}, "List<T>"}, {[]interface{}{}, "List<T>"},
	} {
		params := map[string]interface{}{"m": tc.value}
		for query, message := range map[string]string{
			"RETURN $m.a AS a":                                "Type mismatch for parameter 'm': " + expected[len("Type mismatch: "):] + tc.typeName,
			"MATCH (n) WHERE $m.a = 1 RETURN n":               "Type mismatch for parameter 'm': " + expected[len("Type mismatch: "):] + tc.typeName,
			"WITH $m AS m RETURN m.a AS a":                    expected + tc.typeName,
			"UNWIND [1] AS i WITH $m AS m, i RETURN m.a AS a": expected + tc.typeName,
			"WITH $m AS m WITH m AS k RETURN k.a AS a":        expected + tc.typeName,
		} {
			_, err := exec.Execute(ctx, query, params)
			require.ErrorContains(t, err, message, "%s %#v", query, tc.value)
		}
	}
	_, err = exec.Execute(ctx, "WITH $m AS m RETURN m.a AS a", map[string]interface{}{"m": 1.5})
	require.ErrorContains(t, err, "Neo.ClientError.Statement.TypeError: Type mismatch: expected a map but was Double(1.500000e+00)")
	_, err = exec.Execute(ctx, "RETURN $m - 1 AS a", map[string]interface{}{"m": []interface{}{"a"}})
	require.ErrorContains(t, err, "Type mismatch for parameter 'm': expected Float, Integer, Duration, Date, Time, LocalTime, LocalDateTime or DateTime but was List<String>")
	for _, value := range []interface{}{map[string]interface{}{"a": int64(5)}, nil} {
		result, err := exec.Execute(ctx, "WITH $m AS m RETURN m.a AS a", map[string]interface{}{"m": value})
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	}
}

// TestUnwindParameterPropertyAccessMatchesNeo4j: UNWIND $m AS x binds the
// list's elements. A list of strings gives them the static type String,
// which a property access rejects when the statement compiles; the elements
// of any other list are checked when they are read. Expectations are Neo4j
// 5.26's answers. Only the list-of-strings case makes the per-execution
// check parse the statement.
func TestUnwindParameterPropertyAccessMatchesNeo4j(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	const query = "UNWIND $m AS x RETURN x.a AS a"
	_, err := exec.Execute(ctx, query, map[string]interface{}{"m": []interface{}{"a"}})
	require.ErrorContains(t, err, "Type mismatch: expected Map, Node, Relationship, Point, Duration, Date, Time, LocalTime, LocalDateTime or DateTime but was String")
	_, err = exec.Execute(ctx, query, map[string]interface{}{"m": []interface{}{int64(1), int64(2)}})
	require.ErrorContains(t, err, "Type mismatch: expected a map but was Long(1)")
	result, err := exec.Execute(ctx, query, map[string]interface{}{"m": []interface{}{map[string]interface{}{"a": int64(1)}}})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)

	rows := map[string]interface{}{"rows": []interface{}{map[string]interface{}{"id": int64(1)}}}
	require.False(t, parameterMayRejectPropertyAccess("UNWIND $rows AS row MERGE (n:N {id: row.id})", rows))
	require.True(t, parameterMayRejectPropertyAccess("UNWIND $m AS x RETURN x.a", map[string]interface{}{"m": []string{"a"}}))
	require.True(t, parameterMayRejectPropertyAccess("WITH $rows AS r RETURN r.id", rows))
}
