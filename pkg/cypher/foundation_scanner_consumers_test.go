package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFoundationScannerConsumers(t *testing.T) {
	e := NewStorageExecutor(newTestMemoryEngine(t))
	t.Run("relationship punctuation is opaque", func(t *testing.T) {
		for _, pattern := range []string{
			"(a {note: ']-]-'})-[r:R]->(b)",
			"(a) /* ]- ]- */ -[r:R]->(b)",
			"(a)-[r:`R]-]-`]->(b)",
		} {
			require.False(t, e.isChainedPattern(pattern), pattern)
		}
		require.True(t, e.isChainedPattern("(a)-[:R]->(b)-[:S]->(c)"))
	})
	t.Run("projection and order splitting", func(t *testing.T) {
		for _, source := range []string{"n.`a,b`, n.x", "n.a /* , ) */, n.x", "[1,2], n.x", "{a:1,b:2}, n.x", "'a,b', 1e-3"} {
			require.Len(t, splitTopLevelComma(source), 2, source)
		}
		require.Len(t, e.parseOrderBySpecs("n.`a,b`, n.x DESC", []string{"n.`a,b`", "n.x"}), 2)
		specs := e.parseOrderBySpecs("n.`a b` DESC /* , */, n.x", []string{"n.`a b`", "n.x"})
		require.Len(t, specs, 2)
		require.True(t, specs[0].descending)
		nodes := []*storage.Node{{Properties: map[string]interface{}{"a,b": int64(2)}}, {Properties: map[string]interface{}{"a,b": int64(1)}}}
		require.EqualValues(t, 1, e.orderNodes(nodes, "n", "n.`a,b`")[0].Properties["a,b"])
		indexed, ok := e.parseNodeOrderSpecs("n.`a,b` DESC /* , */, n.x", "n")
		require.True(t, ok)
		require.Len(t, indexed, 2)
		require.Equal(t, "a,b", indexed[0].propName)
	})
	t.Run("list token closure and exponents", func(t *testing.T) {
		for _, source := range []string{"'open", "\"open"} {
			require.NotPanics(t, func() {
				end, kind, _ := staticListOperand(source, 0, nil)
				require.LessOrEqual(t, end, len(source))
				require.Empty(t, kind)
			})
		}
		for _, source := range []string{"'a\\'b'", "\"a,b\"", "1e-3", "2E+4"} {
			end, _, _ := staticListOperand(source+" IN []", 0, nil)
			require.Equal(t, len(source), end, source)
		}
		require.Equal(t, []string{"1e-3 ", "+", " 2E+4"}, splitArithmeticExpression("1e-3 + 2E+4"))
	})
	t.Run("relationship readers", func(t *testing.T) {
		pattern := "(a {note:'-[:FAKE]->(bad)'})-[:`R|]` /* ] */]->(`b:c`)"
		hops := e.parseRelationshipHops(pattern, "a")
		require.Len(t, hops, 1)
		require.Equal(t, []string{"R|]"}, hops[0].relTypes)
		require.True(t, hops[0].outgoing)
		require.Equal(t, []string{"R|]"}, e.extractRelTypesFromPattern(pattern, "-["))
		require.Equal(t, "b:c", e.extractTargetVariable(pattern, "a"))
		require.Equal(t, "a:b", e.extractTargetVariable("(`a:b` {note:'('})<-[:R]-(b)", "b"))
	})
	t.Run("AST delete and call", func(t *testing.T) {
		b := NewASTBuilder()
		require.Equal(t, []string{"`a,b`", "c"}, b.parseDelete(cyphertext.NormalizeSource("DELETE `a,b`, c"), false).Variables)
		call := b.parseCall(cyphertext.NormalizeSource("CALL `p(q)`(')') YIELD `a,b`, c"))
		require.Equal(t, "`p(q)`", call.Procedure)
		require.Equal(t, "')'", call.RawArgs)
		require.Equal(t, []string{"`a,b`", "c"}, call.Yield)
	})
}

func TestFoundationScannerPublicConsumers(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
	for _, query := range []string{
		"CREATE (:Foundation {`a,b`: 2, x: 1}), (:Foundation {`a,b`: 1, x: 2})",
		"MATCH (n:Foundation) RETURN n.`a,b`, n.x ORDER BY n.`a,b`, n.x",
		"MATCH (n:Foundation) RETURN n.`a,b`, n.x ORDER BY n.`a,b` /* , ) */, n.x",
		"RETURN [1e-3, 2E+4] AS values, 'a,b' AS text",
	} {
		result, err := e.Execute(t.Context(), query, nil)
		require.NoError(t, err, query)
		require.NotNil(t, result)
		if len(result.Columns) == 2 && len(result.Rows) == 2 {
			require.EqualValues(t, 1, result.Rows[0][0])
			require.EqualValues(t, 2, result.Rows[1][0])
		}
		if query == "RETURN [1e-3, 2E+4] AS values, 'a,b' AS text" {
			require.Len(t, result.Rows, 1)
			require.Equal(t, []interface{}{float64(0.001), float64(20000)}, result.Rows[0][0])
			require.Equal(t, "a,b", result.Rows[0][1])
		}
	}
}

func TestFoundationUseDiagnosticAndDynamicControl(t *testing.T) {
	_, _, hasUse, err := parseUseClause(t.Context(), "USE $db RETURN 1", false, cyphertext.RawSource)
	require.True(t, hasUse)
	require.ErrorContains(t, err, "Invalid input '$':")
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	use, rest, hasUse, err := parseUseClause(t.Context(), "USE graph.byName($db) RETURN 1", false, cyphertext.RawSource)
	require.NoError(t, err)
	require.True(t, hasUse)
	require.Equal(t, "graph.byName", use.Function)
	require.Equal(t, []string{"$db"}, use.Args)
	require.Equal(t, "RETURN 1", rest)
}
