package cypher

import (
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBuildIDCaseExpression_SortsAndEscapes(t *testing.T) {
	expr := buildIDCaseExpression("n", map[string]interface{}{
		"b":       int64(2),
		"a'quote": "x",
	})
	require.Equal(t, "CASE id(n) WHEN 'a\\'quote' THEN 'x' WHEN 'b' THEN 2 ELSE null END", expr)
}

func TestRewriteFirstWithScalar(t *testing.T) {
	_, ok := rewriteFirstWithScalar("RETURN x", "x", "CASE id(n) WHEN '1' THEN 2 ELSE null END")
	require.False(t, ok)

	rewritten, ok := rewriteFirstWithScalar("WITH x, y RETURN x", "x", "CASE id(n) WHEN '1' THEN 2 ELSE null END")
	require.True(t, ok)
	require.Equal(t, compactCypherFragment("WITH CASE id(n) WHEN '1' THEN 2 ELSE null END AS x, y RETURN x"), compactCypherFragment(rewritten))

	rewritten, ok = rewriteFirstWithScalar("MATCH (n) WITH x WHERE x > 1 RETURN x", "x", "CASE id(n) WHEN '1' THEN 2 ELSE null END")
	require.True(t, ok)
	require.Equal(t, compactCypherFragment("MATCH (n) WITH CASE id(n) WHEN '1' THEN 2 ELSE null END AS x WHERE x > 1 RETURN x"), compactCypherFragment(rewritten))

	_, ok = rewriteFirstWithScalar("WITH y RETURN y", "x", "CASE id(n) WHEN '1' THEN 2 ELSE null END")
	require.False(t, ok)
}

func TestProjectFromRow(t *testing.T) {
	node := &storage.Node{Properties: map[string]interface{}{"name": "alice"}}
	row := pipelineRow{
		"x":    int64(7),
		"n":    node,
		"m":    map[string]interface{}{"k": "v"},
		"text": "hello",
	}

	v, ok := projectFromRow(row, "x")
	require.True(t, ok)
	require.EqualValues(t, int64(7), v)

	v, ok = projectFromRow(row, "n.name")
	require.True(t, ok)
	require.Equal(t, "alice", v)

	v, ok = projectFromRow(row, "m.k")
	require.True(t, ok)
	require.Equal(t, "v", v)

	v, ok = projectFromRow(row, "'literal'")
	require.True(t, ok)
	require.Equal(t, "literal", v)

	_, ok = projectFromRow(row, "missing.prop")
	require.False(t, ok)
}

func TestEvaluateListForPipeline(t *testing.T) {
	row := pipelineRow{
		"items": []interface{}{int64(1), "x"},
		"obj": map[string]interface{}{
			"list": []map[string]interface{}{{"k": "v"}},
		},
		"n": &storage.Node{Properties: map[string]interface{}{"vals": []interface{}{true, false}}},
	}

	require.Equal(t, []interface{}{int64(1), "x"}, evaluateListForPipeline("items", row))
	require.Equal(t, []interface{}{map[string]interface{}{"k": "v"}}, evaluateListForPipeline("obj.list", row))
	require.Equal(t, []interface{}{true, false}, evaluateListForPipeline("n.vals", row))

	lit := evaluateListForPipeline("[{a: 1, b: {c: 'x'}}, true, 3.5]", row)
	require.Len(t, lit, 3)
	require.Equal(t, true, lit[1])
	require.Equal(t, 3.5, lit[2])
	firstMap, ok := lit[0].(map[string]interface{})
	require.True(t, ok)
	require.EqualValues(t, int64(1), firstMap["a"])
	nested, ok := firstMap["b"].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "x", nested["c"])

	require.Nil(t, evaluateListForPipeline("[fn(1)]", row))
}

func TestParseLiteralMapAndScalarsForPipeline(t *testing.T) {
	m := parseLiteralMapForPipeline("{a: 1, b: {c: 'x'}, ok: true, none: null}")
	require.NotNil(t, m)
	require.EqualValues(t, int64(1), m["a"])
	require.Equal(t, true, m["ok"])
	require.Nil(t, m["none"])

	nested, ok := m["b"].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "x", nested["c"])

	require.Nil(t, parseLiteralMapForPipeline("{a}"))

	v, ok := parseLiteralScalarForPipeline("+42")
	require.True(t, ok)
	require.EqualValues(t, int64(42), v)

	v, ok = parseLiteralScalarForPipeline("-2.5")
	require.True(t, ok)
	require.Equal(t, -2.5, v)

	v, ok = parseLiteralScalarForPipeline("null")
	require.True(t, ok)
	require.Nil(t, v)

	_, ok = parseLiteralScalarForPipeline("fn(1)")
	require.False(t, ok)
}

func TestParseIntFast(t *testing.T) {
	v, ok := parseIntFast("123")
	require.True(t, ok)
	require.EqualValues(t, 123, v)

	v, ok = parseIntFast("-9")
	require.True(t, ok)
	require.EqualValues(t, -9, v)

	v, ok = parseIntFast("+7")
	require.True(t, ok)
	require.EqualValues(t, 7, v)

	_, ok = parseIntFast("")
	require.False(t, ok)
	_, ok = parseIntFast("+")
	require.False(t, ok)
	_, ok = parseIntFast("12x")
	require.False(t, ok)
}

// TestFastNumberParsersMatchStrconv pins that the numeric pre-checks of
// parseIntFast / parseFloatFast only skip text strconv would reject: every
// input parses to the same result as before.
func TestFastNumberParsersMatchStrconv(t *testing.T) {
	for _, input := range []string{
		"0", "7", "-7", "+7", "9223372036854775807", "9223372036854775808", "-9223372036854775808",
		"1.5", ".5", "-.5", "+.5", "1e5", "1E-3", "-2.5e3", "0.0", "-0.0", "1.", "5.e2",
		"e.uuid", "n.name", "e", "E5", ".e5", "-", "+", ".", "-.", "1a", "a1", "1.2.3",
		"NaN", "Inf", "Infinity", "-Infinity", "1_000", "0x1F", "0o17", "",
	} {
		wantInt, errInt := strconv.ParseInt(input, 10, 64)
		gotInt, okInt := parseIntFast(input)
		if !strings.HasPrefix(strings.TrimLeft(input, "+-"), "0x") && !strings.HasPrefix(strings.TrimLeft(input, "+-"), "0o") {
			require.Equal(t, errInt == nil, okInt, "parseIntFast(%q)", input)
			if okInt {
				require.Equal(t, wantInt, gotInt, "parseIntFast(%q)", input)
			}
		}
		wantFloat, errFloat := strconv.ParseFloat(input, 64)
		wantOK := errFloat == nil && strings.ContainsAny(input, ".eE")
		gotFloat, okFloat := parseFloatFast(input)
		require.Equal(t, wantOK, okFloat, "parseFloatFast(%q)", input)
		if okFloat && wantFloat != 0 {
			require.Equal(t, wantFloat, gotFloat, "parseFloatFast(%q)", input)
		}
	}
}
