package cypher

// #894: a projection body's leading word distinct is the DISTINCT keyword
// whenever what follows can start an expression, and a variable name of the
// same spelling otherwise. The retained WITH / RETURN body reads that decision
// through projectionBodyDistinct, so both the static view and the statement's
// own items see the modifier the body's text has. Where the grammar could only
// parse the statement by keeping the word inside the first item's expression
// (RETURN distinct STARTS WITH 'a' AS v), Neo4j reads the keyword and rejects
// the statement: the word after the modifier is not an expression start. These
// controls cover the decision, its neighbors (operators, aliases, subscripts,
// strings, numbers, an undefined name) and the statement contract in both
// execution modes.

import (
	"context"
	"strings"
	"testing"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/stretchr/testify/require"
)

func TestProjectionBodyDistinctReadsTheRetainedBody(t *testing.T) {
	ctx := context.Background()
	read := func(projection string) (bool, error) {
		query := "RETURN " + projection
		clauses, ok := splitPipelineClausesAllowingProcedureCalls(ctx, query)
		require.True(t, ok, query)
		require.Len(t, clauses, 1, query)
		body := clauses[0].syntax.ChildRule(cypherantlr.CypherParserRULE_projectionBody)
		require.True(t, body.Valid(), query)
		return projectionBodyDistinct(body)
	}
	for _, tc := range []struct {
		projection string
		distinct   bool
		invalid    string
	}{
		// The retained keyword is the modifier.
		{"DISTINCT 1 AS v", true, ""},
		{"DISTINCT distinct STARTS WITH 'a' AS v", true, ""},
		{"DISTINCT *", true, ""},
		// A name spelled distinct, before what can continue an expression.
		{"1 AS v", false, ""},
		{"distinct", false, ""},
		{"distinct * 2 AS v", false, ""},
		{"distinct CONTAINS 'a' AS v", false, ""},
		{"distinct IS NULL AS v", false, ""},
		{"distinct{.a} AS v", false, ""},
		{"distinct ORDER BY distinct", false, ""},
		{"*, 2 AS y", false, ""},
		// The keyword, before what cannot start an expression.
		{"distinct + 1 AS v", true, ""},
		{"distinct[0] AS v", true, ""},
		{"distinct (1) AS v", true, ""},
		// The word operator after the keyword leaves no expression: the
		// retained items kept distinct as a name to make the statement
		// parse, which Neo4j reads as a syntax error at that operator.
		{"distinct STARTS WITH 'a' AS v", true, "Invalid input 'STARTS': expected an expression"},
		{"distinct ENDS WITH 'b' AS v", true, "Invalid input 'ENDS': expected an expression"},
	} {
		distinct, err := read(tc.projection)
		require.Equal(t, tc.distinct, distinct, tc.projection)
		if tc.invalid == "" {
			require.NoError(t, err, tc.projection)
			continue
		}
		require.Error(t, err, tc.projection)
		require.Contains(t, statusText(err), "Neo.ClientError.Statement.SyntaxError", tc.projection)
		require.Contains(t, statusText(err), tc.invalid, tc.projection)
	}
}

func TestProjectionBodyDistinctStatementContract(t *testing.T) {
	for _, mode := range []string{"auto-commit", "explicit transaction"} {
		t.Run(mode, func(t *testing.T) {
			exec := newAsyncStackTestExecutor(t)
			ctx := context.Background()
			run := func(query string) (*ExecuteResult, error) {
				if mode == "auto-commit" {
					return exec.Execute(ctx, query, nil)
				}
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				defer func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) }()
				return exec.Execute(ctx, query, nil)
			}
			for _, tc := range []struct {
				query string
				rows  [][]interface{}
			}{
				// A name spelled distinct, with the neighbors the classifier
				// keeps apart: operators, a subscript, a call-like item and
				// explicit DISTINCT.
				{"WITH 'a' AS distinct RETURN distinct CONTAINS 'a' AS v", [][]interface{}{{true}}},
				// Before + the word is the keyword, so +1 is unary (#894).
				{"WITH 1 AS distinct RETURN distinct + 1 AS v", [][]interface{}{{int64(1)}}},
				{"WITH 2 AS distinct RETURN distinct * 2 AS v", [][]interface{}{{int64(4)}}},
				{"WITH [5] AS distinct RETURN distinct[0] AS v", [][]interface{}{{[]interface{}{int64(0)}}}},
				{"WITH 2 AS distinct RETURN distinct (1) AS v", [][]interface{}{{int64(1)}}},
				{"WITH 1 AS distinct RETURN distinct", [][]interface{}{{int64(1)}}},
				{"WITH 1 AS distinct RETURN DISTINCT distinct", [][]interface{}{{int64(1)}}},
				{"WITH 1 AS distinct RETURN DISTINCT 1 + 1 AS v", [][]interface{}{{int64(2)}}},
				{"WITH 'a' AS distinct RETURN DISTINCT distinct STARTS WITH 'a' AS v", [][]interface{}{{true}}},
				{"WITH DISTINCT 1 AS v RETURN v", [][]interface{}{{int64(1)}}},
			} {
				result, err := run(tc.query)
				require.NoError(t, err, tc.query)
				require.Equal(t, tc.rows, result.Rows, tc.query)
			}
			for _, query := range []string{
				// Before a word operator, distinct is the keyword and the
				// item has no left operand.
				"WITH 'a' AS distinct RETURN distinct STARTS WITH 'a' AS v",
				"WITH 'ab' AS distinct RETURN distinct ENDS WITH 'b' AS v",
				"WITH 'a' AS distinct RETURN distinct STARTS WITH distinct AS v",
				// Neighboring undefined names and a bare word operator.
				"WITH 2 AS x RETURN distinct STARTS WITH 'a' AS v",
				"WITH 'a' AS distinct RETURN DISTINCT STARTS WITH 'a' AS v",
				"WITH 'a' AS x RETURN STARTS WITH 'a' AS v",
			} {
				_, err := run(query)
				require.Error(t, err, query)
				require.True(t, strings.HasPrefix(statusText(err), "Neo.ClientError.Statement.SyntaxError"), "%s: %s", query, statusText(err))
			}
		})
	}
}
