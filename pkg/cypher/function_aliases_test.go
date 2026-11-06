package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The GQL function aliases are read the way the client wrote them: backticks
// quote a name rather than spell data (`ceiling`(1.2) calls ceiling), and a
// comment between the name and its arguments, or between PROPERTY_EXISTS's
// arguments, is a gap rather than the end of a call.
func TestFunctionAliasCallNameReading(t *testing.T) {
	for query, want := range map[string]string{
		"RETURN ceiling(1.2) AS v":   "RETURN ceil(1.2) AS v",
		"RETURN Ceiling(1.2) AS v":   "RETURN ceil(1.2) AS v",
		"RETURN `ceiling`(1.2) AS v": "RETURN ceil(1.2) AS v",
		"RETURN `CEILING`(1.2) AS v": "RETURN ceil(1.2) AS v",
		"RETURN `ln`(1) AS v":        "RETURN log(1) AS v",
		// A comment between the name and its arguments separates them, as
		// spaces do; a string inside that comment stays data.
		"RETURN ceiling /* don't range('bad'), */ (1.2) AS v": "RETURN ceil /* don't range('bad'), */ (1.2) AS v",
		"RETURN `ceiling`(/* comment */ 1.2) AS v":            "RETURN ceil(/* comment */ 1.2) AS v",
		"RETURN ln // log\n (1) AS v":                         "RETURN log // log\n (1) AS v",
		"RETURN collect_list /* c */ (x) AS v":                "RETURN collect /* c */ (x) AS v",
		// PROPERTY_EXISTS(n, key) writes the key as a string whatever separates
		// its arguments, and whether the key is written bare or quoted.
		"MATCH (n) WHERE PROPERTY_EXISTS(n, x) RETURN n":                   "MATCH (n) WHERE PROPERTY_EXISTS(n, 'x') RETURN n",
		"MATCH (n) WHERE PROPERTY_EXISTS(n /* c */, /* c */ x) RETURN n":   "MATCH (n) WHERE PROPERTY_EXISTS(n /* c */, /* c */ 'x') RETURN n",
		"MATCH (n) WHERE PROPERTY_EXISTS(n, `x`) RETURN n":                 "MATCH (n) WHERE PROPERTY_EXISTS(n, 'x') RETURN n",
		"MATCH (n) WHERE PROPERTY_EXISTS(`n` /* c */, x /* c */) RETURN n": "MATCH (n) WHERE PROPERTY_EXISTS(`n` /* c */, 'x' /* c */) RETURN n",
		"MATCH (n) WHERE `property_exists`(n, `it's`) RETURN n":            "MATCH (n) WHERE `property_exists`(n, 'it\\'s') RETURN n",
		"MATCH (n) WHERE `property_exists` /* c */ (n, x) RETURN n":        "MATCH (n) WHERE `property_exists` /* c */ (n, 'x') RETURN n",
	} {
		rewritten, rewrite := canonicalizeFunctionAliases(query)
		require.NotNil(t, rewrite, query)
		require.Equal(t, want, rewritten, query)
		require.Equal(t, query, rewrite.Source.Original, "the rewrite keeps the client's text")
	}
}

// A quoted name is a name, not data: it names the same functions a bare name
// does, and a name that continues a longer name, a property, a parameter or a
// string is neither. Those stay as written.
func TestFunctionAliasCallNamesNotData(t *testing.T) {
	for _, query := range []string{
		"RETURN 'ceiling(1.2)' AS v",
		"RETURN \"ln(1)\" AS v",
		"RETURN 'x`ceiling`(1.2)' AS v",
		"RETURN '`ln`(1)' AS v",
		"WITH {ln: 1} AS m RETURN m.ln AS v",
		"WITH 1 AS ln RETURN ln AS v",
		"WITH 1 AS ln RETURN ln /* c */ AS v",
		"RETURN $ln AS v",
		"RETURN {p: $ln} AS v",
		"RETURN myceiling(1.2) AS v",
		"RETURN ceyling(1.2) AS v",
		"MATCH (n) RETURN n.`ceiling` AS v",
		"MATCH (n) RETURN n.`ceiling` (1) AS v",
		"RETURN `my ceiling`(1) AS v",
		"RETURN custom . /* c */ `ceiling`(1) AS v",
		"RETURN custom /* c */ . ceiling(1) AS v",
		"RETURN `cei``ling`(1) AS v",
		"RETURN `ceiling` AS v",
		"MATCH (n) RETURN n /* c */ AS v",
		"MATCH (n) RETURN PROPERTY_EXISTS(n.s, s) AS v",
		"MATCH (n) RETURN PROPERTY_EXISTS(n, s t) AS v",
		"MATCH (n) RETURN PROPERTY_EXISTS(n, 's') AS v",
	} {
		rewritten, rewrite := canonicalizeFunctionAliases(query)
		require.Nil(t, rewrite, query)
		require.Equal(t, query, rewritten, query)
	}
}

// The public path: the same statements answer as the catalog's functions do
// under both parsers, with query normalization on and off.
func TestFunctionAliasCallEvaluation(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "function_aliases"))
		ctx := context.Background()
		_, err := exec.Execute(ctx, "CREATE (:FA {id: 1, x: 1})", nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, "MATCH (n:FA) SET n.`it's` = 2, n.`sp ace` = 3", nil)
		require.NoError(t, err)
		run := func(query string) *ExecuteResult {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			require.Len(t, result.Rows, 1, query)
			return result
		}
		for query, want := range map[string]interface{}{
			"RETURN ceiling /* don't range('bad'), */ (1.2) AS v":                     2.0,
			"RETURN `ceiling`(/* comment */ 1.2) AS v":                                2.0,
			"RETURN `ceiling`(1.2) AS v":                                              2.0,
			"RETURN `ln`(1) AS v":                                                     0.0,
			"UNWIND [1, 2] AS x RETURN collect_list /* c */ (x) AS v":                 []interface{}{int64(1), int64(2)},
			"UNWIND [1, 2] AS x RETURN `collect_list`(DISTINCT x) AS v":               []interface{}{int64(1), int64(2)},
			"MATCH (n:FA) RETURN `ceiling`(n.x + 0.5) AS v":                           2.0,
			"RETURN 'ceiling(1.2)' AS v":                                              "ceiling(1.2)",
			"RETURN '`ln`(1)' AS v":                                                   "`ln`(1)",
			"MATCH (n:FA) RETURN PROPERTY_EXISTS(n, x) AS v":                          true,
			"MATCH (n:FA) RETURN PROPERTY_EXISTS(n /* c */, x) AS v":                  true,
			"MATCH (n:FA) RETURN PROPERTY_EXISTS(n, /* c */ x) AS v":                  true,
			"MATCH (n:FA) RETURN PROPERTY_EXISTS(n, 'missing') AS v":                  false,
			"MATCH (n:FA) WHERE PROPERTY_EXISTS(n, `x`) RETURN n.id AS v":             int64(1),
			"MATCH (n:FA) WHERE `property_exists`(n, x) RETURN n.id AS v":             int64(1),
			"MATCH (n:FA) WHERE property_exists /* c */ (n, `it's`) RETURN n.id AS v": int64(1),
			"MATCH (n:FA) WHERE PROPERTY_EXISTS(n, `sp ace`) RETURN n.id AS v":        int64(1),
		} {
			require.Equal(t, want, run(query).Rows[0][0], query)
		}
		// A call's column keeps the text the client wrote, quotes included.
		result := run("RETURN `ceiling`(1.2)")
		require.Equal(t, []string{"`ceiling`(1.2)"}, result.Columns)
		require.Equal(t, 2.0, result.Rows[0][0])
		result = run("MATCH (n:FA) RETURN PROPERTY_EXISTS(n, x)")
		require.Equal(t, []string{"PROPERTY_EXISTS(n, x)"}, result.Columns)
		require.Equal(t, true, result.Rows[0][0])
	})
}
