package cypher

// Neo4j 5 statement framing (#743, #744): the CYPHER … query-option preamble
// runs the statement it precedes; a trailing FINISH (on every UNION branch)
// runs the statement and returns no rows; EXPLAIN and PROFILE are mutually
// exclusive.

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func framingExec(t *testing.T, name string) *StorageExecutor {
	t.Helper()
	return NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "framing_"+name))
}

func TestCypherPreambleRunsTheStatement(t *testing.T) {
	exec := framingExec(t, "preamble")
	ctx := context.Background()
	cases := []struct {
		query string
		value interface{}
	}{
		{"CYPHER 5 RETURN 1 AS x", int64(1)},
		{"CYPHER RETURN 1 AS x", int64(1)},
		{"CYPHER 5 CYPHER 5 RETURN 1 AS x", int64(1)},
		{"cypher RETURN 1 AS x", int64(1)},
		{"CYPHER runtime=slotted RETURN 1 AS x", int64(1)},
		{"CYPHER runtime = slotted RETURN 1 AS x", int64(1)},
		{"CYPHER planner=cost runtime=slotted RETURN 1 AS x", int64(1)},
		{"CYPHER 5 planner=dp RETURN 2 AS x", int64(2)},
		{"CYPHER expressionEngine=interpreted RETURN 3 AS x", int64(3)},
		{"CYPHER CYPHER 5 RETURN 4 AS x", int64(4)},
		{"  CYPHER 5 RETURN 5 AS x", int64(5)},
	}
	for _, tc := range cases {
		res, err := exec.Execute(ctx, tc.query, nil)
		require.NoError(t, err, tc.query)
		require.Equal(t, [][]interface{}{{tc.value}}, res.Rows, tc.query)
	}

	// A preamble before a write runs the write.
	_, err := exec.Execute(ctx, "CYPHER 5 CREATE (n:Framing {v: 1})", nil)
	require.NoError(t, err)
	rows, err := exec.Execute(ctx, "MATCH (n:Framing) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), rows.Rows[0][0])

	// A preamble without a statement is still a syntax error.
	_, err = exec.Execute(ctx, "CYPHER 5", nil)
	require.Error(t, err)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
}

func TestExplainProfileConflictRejected(t *testing.T) {
	exec := framingExec(t, "explainprofile")
	ctx := context.Background()
	for _, query := range []string{
		"EXPLAIN PROFILE RETURN 1",
		"EXPLAIN PROFILE MATCH (n) RETURN n",
		"PROFILE EXPLAIN MATCH (n) RETURN n",
		"EXPLAIN CYPHER 5 PROFILE RETURN 1",
		"PROFILE CYPHER 5 EXPLAIN RETURN 1",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.ArgumentError", code, query)
	}
	// Each alone still runs.
	for _, query := range []string{"EXPLAIN MATCH (n) RETURN n", "PROFILE MATCH (n) RETURN n"} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
}

func TestMonsterStatementBoundaries(t *testing.T) {
	for _, mode := range []string{"nornic", "antlr"} {
		t.Run(mode, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(mode)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, query := range []string{
				"CREATE (:Semi); MATCH (n:Semi) RETURN count(n) AS c",
				"RETURN 1 AS x;;",
				"CREATE (:Semi) RETURN 1 AS x UNION RETURN 2 AS y",
				"CREATE (:Semi) RETURN 1 AS x UNION ALL RETURN 2 AS y",
				"RETURN 1 AS x UNION FINISH",
				"CREATE (:Semi) RETURN 1 AS x UNION FINISH",
				"FINISH UNION RETURN 1 AS x",
			} {
				t.Run(query, func(t *testing.T) {
					exec := framingExec(t, "boundaries")
					_, err := exec.Execute(context.Background(), query, nil)
					require.Error(t, err)
					code, _ := nornicerrors.Neo4jStatus(err)
					require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
					result, err := exec.Execute(context.Background(), "MATCH (n:Semi) RETURN count(n)", nil)
					require.NoError(t, err)
					require.Equal(t, int64(0), result.Rows[0][0])
				})
			}
		})
	}
}

func TestResidualANTLRLiteralAndSyntaxDetails(t *testing.T) {
	for _, mode := range []string{"nornic", "antlr"} {
		t.Run(mode, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(mode)
			t.Cleanup(func() { config.SetParserType(previous) })
			exec := framingExec(t, "antlr_literals")
			for _, test := range []struct {
				query string
				want  []interface{}
			}{
				{"RETURN -0x1 AS x", []interface{}{int64(-1)}},
				{"RETURN -0o7 AS x", []interface{}{int64(-7)}},
				{"RETURN 3-1 AS x, 3.5-1.5 AS y", []interface{}{int64(2), float64(2)}},
				{"RETURN NOT NOT true AS t, NOT NOT false AS f, NOT NOT null AS n", []interface{}{true, false, nil}},
				{"RETURN [-0x1, 0x2] AS xs", []interface{}{[]interface{}{int64(-1), int64(2)}}},
				{"RETURN [2E-01, 'text', null, 71034856, false] AS literal", []interface{}{[]interface{}{float64(0.2), "text", nil, int64(71034856), false}}},
				{"RETURN 1e+003 AS x, -2e-001 AS y", []interface{}{float64(1000), float64(-0.2)}},
			} {
				t.Run(test.query, func(t *testing.T) {
					result, err := exec.Execute(context.Background(), test.query, nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{test.want}, result.Rows)
				})
			}
			for _, query := range []string{"RETURN [,]", "RETURN {,}", "RETURN [[1], [2]"} {
				t.Run(query, func(t *testing.T) {
					_, err := exec.Execute(context.Background(), query, nil)
					require.Error(t, err)
					if mode == "antlr" {
						requireMatchSemanticDetail(t, err, "UnexpectedSyntax")
					}
				})
			}
			_, err := exec.Execute(context.Background(), "MATCH (a)-[:LIKES..]->(c) RETURN a", nil)
			requireMatchSemanticDetail(t, err, "InvalidRelationshipPattern")
		})
	}
}

func TestResidualLabelPredicateReferences(t *testing.T) {
	for _, test := range []struct {
		expression string
		want       []string
	}{
		{"count(CASE WHEN n:Ignored THEN 1 END)", []string{"n"}},
		{"n:First:Second", []string{"n"}},
		{"n:`Ignored value`", []string{"n"}},
		{"{a: missing}", []string{"missing"}},
		{"{a: n:Ignored}", []string{"n"}},
		{"coalesce({a: n:Ignored}, n:Other)", []string{"n", "n"}},
		{"coalesce(n:Ignored, missing)", []string{"n", "missing"}},
	} {
		t.Run(test.expression, func(t *testing.T) {
			require.Equal(t, test.want, expressionFreeVariables(test.expression))
		})
	}
	for _, mode := range []string{"nornic", "antlr"} {
		t.Run(mode, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(mode)
			t.Cleanup(func() { config.SetParserType(previous) })
			exec := framingExec(t, "label_predicate")
			_, err := exec.Execute(context.Background(), "CREATE (:Ignored), (:Other)", nil)
			require.NoError(t, err)
			result, err := exec.Execute(context.Background(), "MATCH (n) RETURN count(CASE WHEN n:Ignored THEN 1 END) AS count", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
			_, err = exec.Execute(context.Background(), "MATCH (n) RETURN count(CASE WHEN missing:Ignored THEN 1 END)", nil)
			requireMatchSemanticDetail(t, err, "UndefinedVariable")
		})
	}
}

func TestResidualMultipartSubqueryGrammar(t *testing.T) {
	for _, mode := range []string{"nornic", "antlr"} {
		t.Run(mode, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(mode)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, query := range []string{
				"MATCH (n:T) CALL { WITH n WITH n WHERE n.id = 2 DETACH DELETE n } RETURN count(*) AS c",
				"MATCH (n:T) CALL { WITH n WITH n WHERE n.id = 2 SET n.k = 1 } RETURN count(*) AS c",
				"MATCH (n:T) CALL (n) { WITH n UNWIND [1, 2] AS value WITH n, value WHERE n.id = 2 SET n.k = value } RETURN n.id AS id ORDER BY id",
				"MATCH (n:T) CALL (n) { WITH n WHERE n.id = 2 SET n.k = 1 } IN TRANSACTIONS OF 1 ROW RETURN n.id AS id ORDER BY id",
				"MATCH (n:T) CALL { WITH n WITH n WHERE n.id = 2 SET n.k = 1 } IN TRANSACTIONS OF 1 ROW RETURN n.id AS id ORDER BY id",
				"RETURN round(1.25) AS a, substring('abc', 1) AS b, ltrim(' a') AS c, normalize('a', NFC) AS d, trim(LEADING FROM ' a') AS e",
				"WITH 5 AS s RETURN COUNT { WITH s AS s RETURN s } AS r",
				"WITH 5 AS s RETURN COUNT { WITH {a: 1} AS inner RETURN inner.a } AS r",
			} {
				t.Run(query, func(t *testing.T) {
					exec := framingExec(t, "multipart")
					_, err := exec.Execute(context.Background(), "CREATE (:T {id: 1}), (:T {id: 2})", nil)
					require.NoError(t, err)
					_, err = exec.Execute(context.Background(), query, nil)
					require.NoError(t, err)
				})
			}
		})
	}
}

func TestResidualParameterNamedAs(t *testing.T) {
	for _, mode := range []string{"nornic", "antlr"} {
		t.Run(mode, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(mode)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, query := range []string{
				"RETURN $as AS value",
				"WITH $as AS value RETURN value",
				"UNWIND $as AS value RETURN value",
				"UNWIND $as AS k MATCH (n:P) WHERE n.k = k RETURN n.v AS value",
			} {
				t.Run(query, func(t *testing.T) {
					exec := framingExec(t, "paramas")
					_, err := exec.Execute(context.Background(), "CREATE (:P {k: 7, v: 7})", nil)
					require.NoError(t, err)
					result, err := exec.Execute(context.Background(), query, map[string]interface{}{"as": int64(7)})
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
				})
			}
		})
	}
}

func TestResidualScopedCallImports(t *testing.T) {
	for _, mode := range []string{"nornic", "antlr"} {
		t.Run(mode, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(mode)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, query := range []string{
				"WITH 1 AS x CALL () { RETURN x AS y } RETURN y",
				"WITH {y: 1} AS x CALL (x.y) { RETURN 1 AS y } RETURN y",
			} {
				t.Run(query, func(t *testing.T) {
					exec := framingExec(t, "emptyimports")
					_, err := exec.Execute(context.Background(), query, nil)
					require.Error(t, err)
					code, _ := nornicerrors.Neo4jStatus(err)
					require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
				})
			}
		})
	}
}

func TestResidualCypherPreambleAdmission(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			exec := NewStorageExecutor(storage.NewNamespacedEngine(storage.NewMemoryEngine(), "preamble"))
			for _, testCase := range []struct{ prefix, code string }{
				{"CYPHER 5.0", "Neo.ClientError.Statement.ArgumentError"},
				{"CYPHER 3.5", "Neo.ClientError.Statement.ArgumentError"},
				{"CYPHER 4.4", "Neo.ClientError.Statement.ArgumentError"},
				{"CYPHER bogus=foo", "Neo.ClientError.Statement.ArgumentError"},
				{"CYPHER runtime=bogus", "Neo.ClientError.Statement.ArgumentError"},
				{"CYPHER planner=bogus", "Neo.ClientError.Statement.ArgumentError"},
				{"CYPHER expressionEngine=bogus", "Neo.ClientError.Statement.ArgumentError"},
				{"EXPLAIN CYPHER runtime=bogus", "Neo.ClientError.Statement.ArgumentError"},
				{"PROFILE CYPHER runtime=bogus", "Neo.ClientError.Statement.ArgumentError"},
				{"CYPHER foo", "Neo.ClientError.Statement.SyntaxError"},
			} {
				t.Run(testCase.prefix, func(t *testing.T) {
					_, err := exec.Execute(context.Background(), testCase.prefix+" CREATE (:Preamble) RETURN 1 AS x", nil)
					require.Error(t, err)
					var semanticError *SemanticError
					require.ErrorAs(t, err, &semanticError)
					require.Equal(t, testCase.code, semanticError.Code)
					stored, err := exec.Execute(context.Background(), "MATCH (n:Preamble) RETURN count(n)", nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(0)}}, stored.Rows)
				})
			}
		})
	}
}

func TestResidualRepeatedExecutionModes(t *testing.T) {
	for _, mode := range []string{"nornic", "antlr"} {
		t.Run(mode, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(mode)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, prefix := range []string{"EXPLAIN EXPLAIN", "PROFILE PROFILE"} {
				t.Run(prefix, func(t *testing.T) {
					exec := framingExec(t, "repeatedmodes")
					result, err := exec.Execute(context.Background(), prefix+" RETURN 1 AS x", nil)
					require.NoError(t, err)
					if prefix == "EXPLAIN EXPLAIN" {
						require.Empty(t, result.Rows)
					} else {
						require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
					}
				})
			}
		})
	}
}

func TestResidualFinishGraphEffects(t *testing.T) {
	for _, mode := range []string{"nornic", "antlr"} {
		t.Run(mode, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(mode)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, testCase := range []struct {
				query string
				count int64
			}{
				{"FINISH UNION FINISH", 0},
				{"CREATE (:U) FINISH UNION CREATE (:V) FINISH", 2},
				{"CREATE (:U) FINISH UNION ALL CREATE (:V) FINISH", 2},
				{"CALL { CREATE (:U) FINISH } RETURN 1 AS x", 1},
				{"CALL { CREATE (:U) FINISH }", 1},
			} {
				t.Run(testCase.query, func(t *testing.T) {
					exec := framingExec(t, "finishresidual")
					result, err := exec.Execute(context.Background(), testCase.query, nil)
					require.NoError(t, err)
					if testCase.query == "CALL { CREATE (:U) FINISH } RETURN 1 AS x" {
						require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
					} else {
						require.Empty(t, result.Rows)
					}
					stored, err := exec.Execute(context.Background(), "MATCH (n) RETURN count(n)", nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{testCase.count}}, stored.Rows)
				})
			}
			t.Run("scoped SET FINISH", func(t *testing.T) {
				exec := framingExec(t, "setfinish")
				_, err := exec.Execute(context.Background(), "CREATE (:T {k: 0})", nil)
				require.NoError(t, err)
				result, err := exec.Execute(context.Background(), "MATCH (t:T) CALL (t) { SET t.k = coalesce(t.k, 0) + 1 FINISH } RETURN t.k", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
			})
		})
	}
}

func TestMonsterScopedCallParserModes(t *testing.T) {
	for _, mode := range []string{"nornic", "antlr"} {
		t.Run(mode, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(mode)
			t.Cleanup(func() { config.SetParserType(previous) })
			exec := framingExec(t, "scopedcall")
			for _, query := range []string{
				"UNWIND [1, 2] AS x CALL (x) { RETURN x * 2 AS y } RETURN y ORDER BY y",
				"UNWIND [1, 2] AS x CALL (*) { RETURN x * 2 AS y } RETURN y ORDER BY y",
				"UNWIND [1, 2] AS x WITH x, 2 AS factor CALL (x, factor) { RETURN x * factor AS y } RETURN y ORDER BY y",
			} {
				t.Run(query, func(t *testing.T) {
					result, err := exec.Execute(context.Background(), query, nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(2)}, {int64(4)}}, result.Rows)
				})
			}
			result, err := exec.Execute(context.Background(), "UNWIND [1, 2] AS x CALL () { RETURN 3 AS y } RETURN y", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(3)}, {int64(3)}}, result.Rows)
		})
	}
}

func TestMonsterStatementFramingLexicalBoundaries(t *testing.T) {
	exec := framingExec(t, "lexical")
	require.Equal(t, []string{"RETURN 1 /* ; */", " RETURN 2 / 1 // ;\n"},
		exec.splitBySemicolon("RETURN 1 /* ; */; RETURN 2 / 1 // ;\n"))
	require.Empty(t, exec.splitBySemicolon(""))
	require.Equal(t, []string{"RETURN 1"}, exec.splitBySemicolon("RETURN 1;"))
	require.Equal(t, []string{"RETURN `semi;colon`", "RETURN ';'"},
		exec.splitBySemicolon("RETURN `semi;colon`;RETURN ';'"))
	for _, query := range []string{
		"RETURN ';' AS x;",
		"RETURN 1 AS `semi;colon`;",
		"RETURN 1 AS x; /* ; ignored */",
		"RETURN 1 AS x // ; ignored\n",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err, query)
	}
	for query, count := range map[string]int{
		"RETURN 1 AS `semi;colon`; RETURN 2 AS x":       2,
		"RETURN ';' AS x; RETURN 2 AS y; RETURN 3 AS z": 3,
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.ErrorContains(t, err, fmt.Sprintf("Expected exactly one statement per query but got: %d", count), query)
	}
}

func TestFinishTerminatorReturnsNoRows(t *testing.T) {
	exec := framingExec(t, "finish")
	ctx := context.Background()

	assertNoRows := func(t *testing.T, query string) *ExecuteResult {
		t.Helper()
		res, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.NotNil(t, res, query)
		require.Empty(t, res.Rows, query)
		require.Empty(t, res.Columns, query)
		return res
	}
	count := func(t *testing.T, query string) int64 {
		t.Helper()
		res, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return res.Rows[0][0].(int64)
	}

	assertNoRows(t, "FINISH")
	assertNoRows(t, "CYPHER 5 FINISH")
	_, err := exec.Execute(ctx, "CREATE (:Fin {v: 1})", nil)
	require.NoError(t, err)
	assertNoRows(t, "MATCH (n:Fin) FINISH")
	assertNoRows(t, "CREATE (:FinA) FINISH")
	require.Equal(t, int64(1), count(t, "MATCH (n:FinA) RETURN count(n)"))
	assertNoRows(t, "UNWIND [1, 2] AS x CREATE (:Fin2 {x: x}) FINISH")
	require.Equal(t, int64(2), count(t, "MATCH (n:Fin2) RETURN count(n)"))
	assertNoRows(t, "MATCH (n:Fin2) DETACH DELETE n FINISH")
	require.Equal(t, int64(0), count(t, "MATCH (n:Fin2) RETURN count(n)"))
	assertNoRows(t, "MATCH (n) FINISH UNION MATCH (m) FINISH")
	assertNoRows(t, "CREATE (:FinUnionA) UNION CREATE (:FinUnionB) FINISH")
	require.Equal(t, int64(1), count(t, "MATCH (n:FinUnionA) RETURN count(n)"))
	require.Equal(t, int64(1), count(t, "MATCH (n:FinUnionB) RETURN count(n)"))
	assertNoRows(t, "CREATE (:FinUnionC) FINISH UNION CREATE (:FinUnionD)")
	require.Equal(t, int64(1), count(t, "MATCH (n:FinUnionC) RETURN count(n)"))
	require.Equal(t, int64(1), count(t, "MATCH (n:FinUnionD) RETURN count(n)"))
	assertNoRows(t, "CALL { CREATE (:Fin3) FINISH }")
	require.Equal(t, int64(1), count(t, "MATCH (n:Fin3) RETURN count(n)"))

	// A quoted 'FINISH' is a value, not a terminator.
	res, err := exec.Execute(ctx, "RETURN 'FINISH' AS s", nil)
	require.NoError(t, err)
	require.Equal(t, "FINISH", res.Rows[0][0])

	// FINISH after WITH or YIELD ends the statement like after any other
	// clause, with its writes, as in Neo4j 5.26.30 (#907).
	_, err = exec.Execute(ctx, "CREATE (:FinW {v: 1})", nil)
	require.NoError(t, err)
	for _, query := range []string{
		"WITH 1 AS x FINISH",
		"UNWIND [1] AS x WITH x ORDER BY x FINISH",
		"WITH 1 AS x WHERE x = 1 FINISH",
		"WITH 1 AS x WITH x FINISH",
		"UNWIND [1, 2] AS x FINISH",
		"CALL db.labels() YIELD label FINISH",
		"CALL db.labels() YIELD label WHERE label = 'x' FINISH",
		"WITH 1 AS x FINISH UNION WITH 2 AS x FINISH",
	} {
		assertNoRows(t, query)
	}
	assertNoRows(t, "CREATE (n:FinW {v: 1}) WITH n FINISH")
	require.Equal(t, int64(2), count(t, "MATCH (n:FinW) RETURN count(n)"))
	assertNoRows(t, "MATCH (n:FinW) WITH n SET n.v = 2 WITH n FINISH")
	require.Equal(t, int64(2), count(t, "MATCH (n:FinW {v: 2}) RETURN count(n)"))
	// A subquery ending in WITH … FINISH is a unit subquery.
	res, err = exec.Execute(ctx, "CALL () { WITH 1 AS x FINISH } RETURN 1 AS one", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, res.Rows)
	res, err = exec.Execute(ctx, "UNWIND [1, 2] AS i CALL { WITH i CREATE (:FinW2 {i: i}) WITH i FINISH } RETURN count(*) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, res.Rows)
	require.Equal(t, int64(2), count(t, "MATCH (n:FinW2) RETURN count(n)"))

	// Statements the executor runs internally follow the same rule.
	res, err = exec.executeInternal(ctx, "CALL db.labels() YIELD label WHERE label = 'x' FINISH", nil)
	require.NoError(t, err)
	require.Empty(t, res.Rows)
	// Only a CALL with a scope clause reads its body's leading WITH as a
	// projection; an unscoped one still checks it as an import list.
	require.True(t, callSubqueryHasScopeClause("CALL () { WITH 1 AS x }"))
	require.False(t, callSubqueryHasScopeClause("CALL { WITH 1 AS x }"))
	require.False(t, callSubqueryHasScopeClause("MATCH (n) CALL () { RETURN 1 }"))
	_, err = exec.Execute(ctx, "UNWIND [1] AS i CALL { WITH i } RETURN 1 AS one", nil)
	require.Error(t, err)
	_, _, handled, err := exec.pipelineApplyCallSubquery(ctx, []pipelineRow{{"i": int64(1)}}, "CALL { WITH i }")
	require.True(t, handled)
	require.Error(t, err)

	// FINISH must be last, and can't follow RETURN: these are syntax
	// errors, as in Neo4j. FINISH is not reserved, so `finish` as a variable
	// (WITH 1 AS finish RETURN finish) is valid; TestFinishAsNameMatchesNeo4j
	// covers it (#958).
	for _, query := range []string{
		"FINISH RETURN 1",
		"MATCH (n:Fin) RETURN n FINISH",
		"RETURN 1 AS x FINISH",
		"WITH 1 AS x RETURN x FINISH",
		"WITH 1 AS x FINISH UNION ALL RETURN 2 AS x",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
	}
}
