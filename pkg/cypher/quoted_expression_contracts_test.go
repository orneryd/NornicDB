package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newQuotedExpressionExecutor is the public executor the residuals of report 26 are
// reproduced on: public Execute over a namespaced memory engine.
func newQuotedExpressionExecutor(t *testing.T) (*StorageExecutor, context.Context) {
	t.Helper()
	return NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "quoted_expressions")), context.Background()
}

// TestQuotedExpressionCreateIndexQuotedBracketTarget pins a CREATE INDEX FOR pattern
// whose label or relationship type is a quoted name holding '[' — the bracket
// is part of the name, not the relationship section. A relationship FOR keeps
// reading its bracketed type, an escaped backtick stays inside the name, and a
// malformed target is still rejected.
func TestQuotedExpressionCreateIndexQuotedBracketTarget(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec, ctx := newQuotedExpressionExecutor(t)
		for _, statement := range []string{
			"CREATE INDEX ei_node_colon FOR (`n:x`:`Document[archived]`) ON (`n:x`.colon_id)",
			"CREATE INDEX ei_edge_colon FOR ()-[`r:x`:`TYPE[archived]`]-() ON (`r:x`.colon_id)",
			"CREATE INDEX ei1_bracket FOR (n:`Document[archived]`) ON (n.id)",
			"CREATE INDEX ei1_escaped FOR (n:`Doc``ument[archived]`) ON (n.id)",
			"CREATE INDEX ei2_bracket FOR ()-[r:`TYPE[archived]`]-() ON (r.id)",
			"CREATE INDEX ei3_relationship FOR (a)-[r:KNOWS]->(b) ON (r.since)",
		} {
			_, err := exec.Execute(ctx, statement, nil)
			require.NoError(t, err, statement)
		}
		names := readSchemaNames(t, ctx, exec, "SHOW INDEXES YIELD name RETURN name")
		for _, name := range []string{"ei1_bracket", "ei1_escaped", "ei2_bracket", "ei3_relationship"} {
			require.Contains(t, names, name)
		}

		// The DDL and the data path agree on the decoded label.
		_, err := exec.Execute(ctx, "CREATE (:`Document[archived]` {id: 1})", nil)
		require.NoError(t, err)
		result, err := exec.Execute(ctx, "MATCH (n:`Document[archived]`) WHERE n.id = 1 RETURN n.id AS id", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)

		// A malformed FOR target is still rejected: the quote never closes.
		_, err = exec.Execute(ctx, "CREATE INDEX ei_bad FOR (n:`Document[archived]` ON (n.id)", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Neo.ClientError.Statement.SyntaxError")
	})
}

// TestQuotedExpressionQuotedNameSubscriptReceiver pins a subscript whose receiver ends
// in a backtick-quoted property name: the receiver is the whole qualification
// (m.`a b`), with or without a comment gap before the bracket. Unquoted
// receivers and out-of-range subscripts keep their existing results.
func TestQuotedExpressionQuotedNameSubscriptReceiver(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec, ctx := newQuotedExpressionExecutor(t)
		for _, tc := range []struct {
			name  string
			query string
			want  interface{}
		}{
			{"multiple quoted properties", "WITH {`a b`:{`c d`:[7]}} AS m RETURN m.`a b`.`c d`[0] AS v", int64(7)},
			{"quoted property", "WITH {`a b`:[7]} AS m RETURN m.`a b`[0] AS v", int64(7)},
			{"comment gap", "WITH {`a b`:[7]} AS m RETURN m.`a b` /*gap*/ [0] AS v", int64(7)},
			{"slice", "WITH {`a b`:[7]} AS m RETURN m.`a b`[0..1] AS v", []interface{}{int64(7)}},
			{"out of range", "WITH {`a b`:[7]} AS m RETURN m.`a b`[1] AS v", nil},
			{"unquoted control", "WITH {a:[7]} AS m RETURN m.a[0] AS v", int64(7)},
			{"quoted key subscript control", "WITH {`a b`:[7]} AS m RETURN (m.`a b`)[0] AS v", int64(7)},
		} {
			t.Run(tc.name, func(t *testing.T) {
				result, err := exec.Execute(ctx, tc.query, nil)
				require.NoError(t, err, tc.query)
				require.Equal(t, [][]interface{}{{tc.want}}, result.Rows, tc.query)
			})
		}
	})
}

// TestQuotedExpressionEscapedAggregateName pins an aggregate called through a
// backtick-quoted name: `count`(x) is count(x) wherever an aggregate is read,
// alone, inside a larger expression, inside a map, and under grouping.
func TestQuotedExpressionEscapedAggregateName(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec, ctx := newQuotedExpressionExecutor(t)
		for _, tc := range []struct {
			name  string
			query string
			want  [][]interface{}
		}{
			{"standalone", "UNWIND [1,2] AS x RETURN `count`(x) AS v", [][]interface{}{{int64(2)}}},
			{"map value", "UNWIND [1,2] AS x RETURN {c:`count`(x)} AS v", [][]interface{}{{map[string]interface{}{"c": int64(2)}}}},
			{"mixed expression", "UNWIND [1,2] AS x RETURN `count`(x) + 1 AS v", [][]interface{}{{int64(3)}}},
			{"grouping key", "UNWIND [1,1,2] AS x RETURN x, `count`(x) AS c ORDER BY x", [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(1)}}},
			{"quoted sum", "UNWIND [1,2] AS x RETURN {c:`sum`(x)} AS v", [][]interface{}{{map[string]interface{}{"c": int64(3)}}}},
			{"empty input identity", "MATCH (n:QuotedExpressionMissing) RETURN `count`(n) AS v", [][]interface{}{{int64(0)}}},
			{"unquoted control", "UNWIND [1,2] AS x RETURN count(x) AS v", [][]interface{}{{int64(2)}}},
			{"quoted scalar control", "RETURN `size`([1,2]) AS v", [][]interface{}{{int64(2)}}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				result, err := exec.Execute(ctx, tc.query, nil)
				require.NoError(t, err, tc.query)
				require.Equal(t, tc.want, result.Rows, tc.query)
			})
		}
	})
}

// TestQuotedExpressionMapProjectionVariableSelector pins a map projection that mixes
// an all-properties selector, a bare variable selector and a literal entry.
// A map literal still requires key-value entries, and a pattern property map
// is a literal even after its variable.
func TestQuotedExpressionMapProjectionVariableSelector(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec, ctx := newQuotedExpressionExecutor(t)
		for _, tc := range []struct {
			name  string
			query string
			want  [][]interface{}
		}{
			{
				"all properties with variable and entry",
				"WITH {name:'Ada'} AS n, 42 AS score RETURN n{.*, score, active:true} AS out",
				[][]interface{}{{map[string]interface{}{"name": "Ada", "score": int64(42), "active": true}}},
			},
			{
				"variable selector with entry",
				"WITH {name:'Ada'} AS n, 42 AS score RETURN n{score, active:true} AS out",
				[][]interface{}{{map[string]interface{}{"score": int64(42), "active": true}}},
			},
			{
				"property selector with variable and entry",
				"WITH {name:'Ada'} AS n, 42 AS score RETURN n{.name, score, active:true} AS out",
				[][]interface{}{{map[string]interface{}{"name": "Ada", "score": int64(42), "active": true}}},
			},
			{
				"variable selector only",
				"WITH {name:'Ada'} AS n, 42 AS score RETURN n{score} AS out",
				[][]interface{}{{map[string]interface{}{"score": int64(42)}}},
			},
			{
				"escaped property selector",
				"WITH {`a b`:1} AS n, 42 AS score RETURN n{.`a b`, score, active:true} AS out",
				[][]interface{}{{map[string]interface{}{"a b": int64(1), "score": int64(42), "active": true}}},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				result, err := exec.Execute(ctx, tc.query, nil)
				require.NoError(t, err, tc.query)
				require.Equal(t, tc.want, result.Rows, tc.query)
			})
		}

		// A map literal's entries are key-value pairs: a bare name is not one.
		for _, query := range []string{
			"RETURN {score, a: 1} AS m",
			"RETURN {a: 1, score} AS m",
		} {
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err, query)
		}
		// A pattern property map is a literal even after its variable.
		_, err := exec.Execute(ctx, "MATCH (n {x}) RETURN n", nil)
		require.Error(t, err)
	})
}

// TestQuotedExpressionMalformedMapEntryRejectedBeforeExecution pins a comma-only map
// entry: `{key: value,}` and `{,}` are syntax errors before any row runs, so an
// empty UNWIND cannot execute a malformed statement. Parse, ASTBuilder and
// Execute reject the same malformed text under both parsers.
func TestQuotedExpressionMalformedMapEntryRejectedBeforeExecution(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec, ctx := newQuotedExpressionExecutor(t)
		for _, query := range []string{
			"UNWIND [] AS row MATCH (a)-->(b {key: a.key,}) RETURN b",
			"MATCH (a)-->(b {key: a.key,}) RETURN b",
			"UNWIND [] AS row MATCH (a)-[r:REPORT26 {k: a.k,}]->(b) RETURN r",
			"UNWIND [] AS row MATCH (a)-->(b {key: a.key}) RETURN b",
		} {
			_, err := exec.Execute(ctx, query, nil)
			if query == "UNWIND [] AS row MATCH (a)-->(b {key: a.key}) RETURN b" {
				require.NoError(t, err, query)
				continue
			}
			require.Error(t, err, query)
			require.Contains(t, err.Error(), "Neo.ClientError.Statement.SyntaxError", query)
		}
	})
}

func TestQuotedExpressionCommaOnlyMapEntriesRejectedOnEverySurface(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec, ctx := newQuotedExpressionExecutor(t)
		for _, query := range []string{
			"RETURN {,} AS m",
			"WITH {a:1} AS n RETURN n{,} AS m",
			"RETURN {a:1, } AS m",
			"WITH {a:1} AS n RETURN n{.*, } AS m",
			"RETURN [,] AS l",
		} {
			// Parse
			if query == "RETURN [,] AS l" {
				// A list literal's comma-only body is the expression reader's
				// syntax error, not a map entry; it must still be rejected.
				_, err := exec.Execute(ctx, query, nil)
				require.Error(t, err, query)
				continue
			}
			_, parseErr := NewParser().Parse(ctx, query)
			require.Error(t, parseErr, query)
			_, astErr := NewASTBuilder().Build(ctx, query)
			require.Error(t, astErr, query)
			_, execErr := exec.Execute(ctx, query, nil)
			require.Error(t, execErr, query)
			require.Contains(t, parseErr.Error(), "Neo.ClientError.Statement.SyntaxError", query)
			require.Contains(t, astErr.Error(), "Neo.ClientError.Statement.SyntaxError", query)
			require.Contains(t, execErr.Error(), "Neo.ClientError.Statement.SyntaxError", query)
		}
		// An empty map and an empty projection stay valid.
		for _, query := range []string{"RETURN {} AS m", "WITH {a:1} AS n RETURN n{} AS m", "WITH {a:1} AS n RETURN n{.*} AS m"} {
			_, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
		}
	})
}
