package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGh657_RemainingReportedDiagnostics(t *testing.T) {
	for _, testCase := range []struct {
		query string
		code  string
	}{
		{"MATCH (n:A) RETURN n.b['a']", "Neo.ClientError.Statement.SyntaxError"},
		{"MATCH (n:A) RETURN n.l['a']", "Neo.ClientError.Statement.SyntaxError"},
		{"MATCH (n:A) RETURN toUpper(n.s * 2)", "Neo.ClientError.Statement.SyntaxError"},
		{"MATCH (n:A) RETURN toUpper(-n.v)", "Neo.ClientError.Statement.SyntaxError"},
		{"MATCH (n:A) RETURN size(n.v * 2)", "Neo.ClientError.Statement.SyntaxError"},
		{"CALL db.index.vector.queryNodes('nope668', 1, [1.0]) YIELD node RETURN node", "Neo.ClientError.Procedure.ProcedureCallFailed"},
		{"RETURN coalesce(a / b, 2)", "Neo.ClientError.Statement.SyntaxError"},
		{"WITH 5 AS v RETURN any(x IN v WHERE x = 5) AS r", "Neo.ClientError.Statement.SyntaxError"},
		{"WITH $m + 1 AS m RETURN m.a", "Neo.ClientError.Statement.SyntaxError"},
		{"WITH 5 AS s RETURN COUNT { UNWIND [{a: 1}] AS s RETURN s.a } AS r", "Neo.ClientError.Statement.SyntaxError"},
		{"WITH 5 AS s RETURN COUNT { WITH {a: 1} AS s RETURN s.a } AS r", "Neo.ClientError.Statement.SyntaxError"},
		{"RETURN toUpper('a', 'b') AS x", "Neo.ClientError.Statement.SyntaxError"},
		{"RETURN toUpper() AS x", "Neo.ClientError.Statement.SyntaxError"},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			executor := setupTestExecutor(t)
			_, err := executor.Execute(context.Background(), testCase.query, nil)
			require.Error(t, err)
			var diagnostic interface{ BoltErrorCode() string }
			require.ErrorAs(t, err, &diagnostic)
			require.Equal(t, testCase.code, diagnostic.BoltErrorCode())
		})
	}
}

func TestGh657_StaticPropertySubscripts(t *testing.T) {
	scope := staticTypeScope{kinds: matchSemanticScope{"n": matchBindingNode}}
	for _, testCase := range []struct {
		expression string
		invalid    bool
	}{
		{"n.l['a']", true},
		{"n.l[0]", false},
		{"n.l[0..1]", false},
		{"n.l[", false},
		{"[]", false},
	} {
		t.Run(testCase.expression, func(t *testing.T) {
			err := validateStaticPropertySubscripts(testCase.expression, scope)
			if testCase.invalid {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestGh657_ValidSemanticControls(t *testing.T) {
	for _, testCase := range []struct {
		query string
		want  []any
	}{
		{"RETURN toUpper('a') AS x", []any{"A"}},
		{"RETURN round(1.25) AS a, substring('abc', 1) AS b, ltrim(' a') AS c, normalize('a', NFC) AS d, trim(LEADING FROM ' a') AS e", []any{float64(1), "bc", "a", "a", "a"}},
		{"MATCH (n:A) RETURN abs(n.v * 2) AS x", []any{int64(10)}},
		{"WITH {a: 1} AS m, [2, 3] AS l RETURN m['a'] AS a, l[0] AS b", []any{int64(1), int64(2)}},
		{"MATCH (n:A) RETURN n.l[0] AS x, n['l'][0] AS y", []any{int64(1), int64(1)}},
		{"WITH 5 AS x, [1, 2] AS v RETURN any(x IN v WHERE x = 2) AS r", []any{true}},
		{"WITH 5 AS s RETURN COUNT { WITH s AS s RETURN s } AS r", []any{int64(1)}},
		{"WITH 5 AS s RETURN COUNT { WITH {a: 1} AS inner RETURN inner.a } AS r", []any{int64(1)}},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			executor := setupTestExecutor(t)
			executeBehaviorQuery(t, executor, "CREATE (:A {l: [1, 2], v: 5})")
			result := executeBehaviorQuery(t, executor, testCase.query)
			require.Len(t, result.Rows, 1)
			require.Len(t, result.Rows[0], len(testCase.want))
			for index, expected := range testCase.want {
				require.EqualValues(t, expected, result.Rows[0][index])
			}
		})
	}
}

func TestMissingIDPropertyReturnsNull(t *testing.T) {
	executor := setupTestExecutor(t)
	executeBehaviorQuery(t, executor, "CREATE (:Document {name: 'without-id'})")

	result := executeBehaviorQuery(t, executor,
		"MATCH (n:Document {name: 'without-id'}) RETURN n.id AS propertyID, id(n) AS internalID")

	require.Len(t, result.Rows, 1)
	require.Nil(t, result.Rows[0][0])
	require.NotNil(t, result.Rows[0][1])
}

func TestNullPredicateOnPropertyOfNullOptionalBinding(t *testing.T) {
	executor := setupTestExecutor(t)

	result := executeBehaviorQuery(t, executor,
		"OPTIONAL MATCH (n) RETURN n.missing IS NULL AS missingIsNull")

	require.Equal(t, [][]interface{}{{true}}, result.Rows)
}

func TestLabelsOnNullOptionalBindingReturnsNull(t *testing.T) {
	executor := setupTestExecutor(t)

	result := executeBehaviorQuery(t, executor,
		"OPTIONAL MATCH (n:DoesNotExist) RETURN labels(n), labels(null)")

	require.Equal(t, [][]interface{}{{nil, nil}}, result.Rows)
}

func TestCollectCaseSkipsNullOptionalBinding(t *testing.T) {
	executor := setupTestExecutor(t)
	executeBehaviorQuery(t, executor, "CREATE (:Seed {id: 'seed'})")

	result := executeBehaviorQuery(t, executor, `
		MATCH (seed:Seed {id: 'seed'})
		OPTIONAL MATCH (seed)-[:RELATES_TO]->(n)
		RETURN collect(CASE WHEN n IS NULL THEN null ELSE {value: n.value} END) AS values`)

	require.Equal(t, [][]interface{}{{[]interface{}{}}}, result.Rows)
}

func TestUnwindCollectCaseSkipsNullOptionalBinding(t *testing.T) {
	executor := setupTestExecutor(t)
	executeBehaviorQuery(t, executor, `
		CREATE (first:Seed {id: 'first', alternate: 'first-alt'}),
		       (second:Seed {id: 'second', alternate: 'second-alt'}),
		       (target:Target {language: 'es', value: 'present'})
		CREATE (first)-[:RELATES_TO]->(target)`)

	result := executeBehaviorQuery(t, executor, `
		UNWIND ['first', 'second'] AS key
		MATCH (seed:Seed) WHERE seed.id = key OR seed.alternate = key
		OPTIONAL MATCH (seed)-[:RELATES_TO]->(n:Target {language: 'es'})
		RETURN key, collect(CASE WHEN n IS NULL THEN null ELSE {
			id: elementId(n),
			language: n.language,
			value: coalesce(n.value, elementId(n))
		} END) AS values`)

	require.Len(t, result.Rows, 2)
	rowsByKey := make(map[string][]interface{}, len(result.Rows))
	for _, row := range result.Rows {
		rowsByKey[row[0].(string)] = row
	}
	require.Len(t, rowsByKey["first"][1], 1)
	require.Equal(t, []interface{}{}, rowsByKey["second"][1])
}
