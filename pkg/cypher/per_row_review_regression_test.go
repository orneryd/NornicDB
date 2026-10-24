package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Reference results verified against Neo4j 5.26.30 on TrueNAS.
func TestPerRowReviewSemantics(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		want        [][]interface{}
	}{
		{"case_keyword_not", "WITH true AS not RETURN CASE WHEN true THEN not END AS v", [][]interface{}{{true}}},
		{"case_keyword_distinct", "WITH true AS distinct RETURN CASE WHEN true THEN distinct END AS v", [][]interface{}{{true}}},
		{"case_not_end_operand", "WITH false AS end RETURN CASE WHEN true THEN NOT end END AS v", [][]interface{}{{true}}},
		{"incoming_pattern", "CREATE (a)-[:R]->(b) WITH a,b WHERE (b)<-[:R]-(a) RETURN true AS v", [][]interface{}{{true}}},
		{"incoming_pattern_not", "CREATE (a)-[:R]->(b) WITH a,b WHERE NOT (b)<-[:R]-(a) RETURN 1 AS v", [][]interface{}{}},
		{"case_context", "RETURN abs(CASE [1,null] WHEN [1,null] THEN 1 ELSE 0 END) AS v", [][]interface{}{{int64(0)}}},
		{"case_row", "RETURN CASE [1,null] WHEN [1,null] THEN 1 ELSE 0 END AS v", [][]interface{}{{int64(0)}}},
		{"comparison_list", "RETURN [1,null] = [1.0,null] AS v", [][]interface{}{{nil}}},
		{"comparison_list_unequal", "RETURN [1,null] <> [2,null] AS v", [][]interface{}{{true}}},
		{"or_error_null_true", "UNWIND [0,0] AS x RETURN 1/x=1 OR null OR x=0 AS v", [][]interface{}{{true}, {true}}},
		{"and_error_null_false", "UNWIND [0,0] AS x RETURN 1/x=1 AND null AND x<>0 AS v", [][]interface{}{{false}, {false}}},
		{"where_and_later_false", "UNWIND [0,0] AS x WITH x WHERE (1/x=1) AND (x<>0) RETURN x", [][]interface{}{}},
		{"or_later_true", "UNWIND [0,0] AS x RETURN (1/x=1) OR (x=0) AS v", [][]interface{}{{true}, {true}}},
		{"case_operand_later_true", "UNWIND [0,0] AS x RETURN CASE WHEN 1/x=1 THEN true END OR x=0 AS v", [][]interface{}{{true}, {true}}},
		{"case_operands_later_true", "UNWIND [0,0] AS x RETURN CASE WHEN 1/x=1 THEN true END OR CASE WHEN x=0 THEN true END AS v", [][]interface{}{{true}, {true}}},
		{"case_operand_later_false", "UNWIND [0,0] AS x RETURN CASE WHEN 1/x=1 THEN true END AND x<>0 AS v", [][]interface{}{{false}, {false}}},
		{"and_later_false", "UNWIND [0,0] AS x RETURN (1/x=1) AND (x<>0) AS v", [][]interface{}{{false}, {false}}},
		{"where_or_later_true", "UNWIND [0,0] AS x WITH x WHERE (1/x=1) OR (x=0) RETURN x", [][]interface{}{{int64(0)}, {int64(0)}}},
		{"case_or_later_true", "UNWIND [0,0] AS x RETURN CASE WHEN (1/x=1) OR (x=0) THEN 1 ELSE 0 END", [][]interface{}{{int64(1)}, {int64(1)}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, text := range []bool{false, true} {
				t.Run(fmt.Sprintf("text=%v", text), func(t *testing.T) {
					rowValueProgramsDisabled.Store(text)
					defer rowValueProgramsDisabled.Store(false)
					exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "review"))
					result, err := exec.Execute(context.Background(), tc.query, nil)
					require.NoError(t, err)
					if len(tc.want) == 0 {
						require.Empty(t, result.Rows)
					} else {
						require.Equal(t, tc.want, result.Rows)
					}
				})
			}
		})
	}
}

func TestPerRowReviewDeletedRelationshipEndpoints(t *testing.T) {
	for _, deletion := range []string{"DELETE r", "DETACH DELETE b"} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/transaction=%v", deletion, explicit), func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "review"))
				ctx := context.Background()
				if explicit {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					defer exec.Execute(ctx, "ROLLBACK", nil)
				}
				query := "CREATE (a {v:1})-[r:R]->(b {v:2}) WITH a,b,r " + deletion + " RETURN startNode(r).v AS s,endNode(r).v AS e"
				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{nil, nil}}, result.Rows)
			})
		}
	}
}

// This oracle never calls another evaluator to derive an expected answer.
// Both compilers are checked against literal values, including lazy CASE errors.
func TestRowPredicateCaseExpectedResults(t *testing.T) {
	exec, _ := perRowTestExecutor(t)
	for _, tc := range []struct {
		expression string
		want       bool
		errorText  string
	}{
		{"CASE WHEN x = 1 THEN true ELSE false END", true, ""},
		{"CASE WHEN x = 2 THEN true ELSE false END", false, ""},
		{"CASE WHEN x = 1 THEN y = 2 ELSE false END OR false", true, ""},
		{"false OR CASE x WHEN 1 THEN true END", true, ""},
		{"x = 1 AND CASE WHEN y = 2 THEN 'yes' END = 'yes'", true, ""},
		{"(CASE WHEN x = 1 THEN 1 ELSE 2 END) + 1 = 2 OR false", true, ""},
		{"CASE [1,null] WHEN [1,null] THEN true ELSE false END", false, ""},
		{"CASE [1,2] WHEN [1.0,2.0] THEN true ELSE false END", true, ""},
		{"CASE null WHEN null THEN true ELSE false END", false, ""},
		{"CASE WHEN null THEN true ELSE false END", false, ""},
		{"true OR CASE WHEN 1/z = 1 THEN true END", true, ""},
		{"false AND CASE WHEN 1/z = 1 THEN true END", false, ""},
		{"CASE WHEN 1/z = 1 THEN true END OR x = 1", true, ""},
		{"CASE WHEN 1/z = 1 THEN true END AND x = 2", false, ""},
		{"CASE WHEN 1/z = 1 THEN true END OR x = 2", false, "/ by zero"},
		{"CASE WHEN x = 1 THEN 1/z ELSE 1 END = 1", false, "/ by zero"},
		{"CASE WHEN x = 2 THEN 1/z ELSE 1 END = 1", true, ""},
		{"CASE WHEN x = 1 THEN 1 END = 1 OR CASE WHEN x = 2 THEN 1 END = 1", true, ""},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			for _, text := range []bool{false, true} {
				rowValueProgramsDisabled.Store(text)
				ctx := withExpressionFailureSlot(context.Background())
				row := map[string]interface{}{"x": int64(1), "y": int64(2), "z": int64(0), rowStatementContextKey: ctx}
				fixtureAdmission1743Context, fixtureAdmission1743 := statementTestContext(ctx, "RETURN "+(tc.expression))
				_ = fixtureAdmission1743Context
				got := compileRowPredicateProgram(tc.expression).holds(fixtureAdmission1743, exec, fixtureAdmission1743Context, row)
				rowValueProgramsDisabled.Store(false)
				if tc.errorText != "" {
					require.ErrorContains(t, getExpressionFailure(ctx), tc.errorText)
					continue
				}
				require.NoError(t, getExpressionFailure(ctx))
				require.Equal(t, tc.want, got)
			}
		})
	}
}

func TestPerRowReviewUndecidedErrorsRemainErrors(t *testing.T) {
	for _, expression := range []string{"1/x=1 OR x<>0", "1/x=1 AND x=0", "1/x=1 AND null", "1/x=1 OR null"} {
		for _, clause := range []string{"RETURN ", "WITH x WHERE "} {
			query := "UNWIND [0,0] AS x " + clause + expression
			if clause != "RETURN " {
				query += " RETURN x"
			}
			t.Run(query, func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "review"))
				_, err := exec.Execute(context.Background(), query, nil)
				require.ErrorContains(t, err, "/ by zero")
			})
		}
	}
}

func TestPerRowReviewSubscriptReceiverBoundary(t *testing.T) {
	for _, expression := range []string{"[1,null] = [1.0,null]", "[1] <> [2]", "x + [1]"} {
		open := closingBracketGroupStart(expression)
		require.Equal(t, -1, rowSubscriptReceiverStart(expression, open), expression)
	}
	for _, expression := range []string{"[1,2][0]", "x[0]", "m[x[0]]", "(x + y)[0]"} {
		require.GreaterOrEqual(t, rowSubscriptStart(expression), 1, expression)
	}
}
