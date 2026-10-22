package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// apoc.convert.toJson writes <, > and & as themselves, as APOC (Jackson)
// does, so JSON text built in Cypher matches other writers' byte for byte: in
// a statement, after UNWIND, and in the expression evaluator.
func TestApocConvertToJSONKeepsHTMLCharacters(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "tojson"))
	ctx := context.Background()
	for query, want := range map[string]interface{}{
		"RETURN apoc.convert.toJson(['a<b&c>']) AS v":                             `["a<b&c>"]`,
		"RETURN apoc.convert.toJson(['plain', '雪']) AS v":                         `["plain","雪"]`,
		"RETURN apoc.convert.toJson({b: 1, a: [1, 'x']}) AS v":                    `{"a":[1,"x"],"b":1}`,
		"UNWIND ['<x & y>'] AS s RETURN apoc.convert.toJson({s: s}) AS v":         `{"s":"<x & y>"}`,
		"WITH {name: 'Tom & Jerry <tj>'} AS m RETURN apoc.convert.toJson(m) AS v": `{"name":"Tom & Jerry <tj>"}`,
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
	fixtureAdmission55Context, fixtureAdmission55 := expressionTestContext(ctx, "apoc.convert.toJson(['a<b&c>'])")
	_ = fixtureAdmission55
	require.Equal(t, `["a<b&c>"]`, exec.evaluateExpressionWithContext(fixtureAdmission55, fixtureAdmission55Context, "apoc.convert.toJson(['a<b&c>'])", nil, nil))

	// U+2028 and U+2029 as themselves, controls escaped, an escaped
	// backslash before the text u2028 left alone.
	result, err := exec.Execute(ctx, "RETURN apoc.convert.toJson($value) AS v", map[string]interface{}{
		"value": []interface{}{"n", "\u2028 \u0001 é \u2029", `back\u2028slash`},
	})
	require.NoError(t, err)
	require.Equal(t, "[\"n\",\"\u2028 \\u0001 é \u2029\",\"back\\\\u2028slash\"]", result.Rows[0][0])
	require.Equal(t, `"\\u2029"`, unescapeLineSeparators(`"\\u2029"`))
	require.Equal(t, `x\`, unescapeLineSeparators(`x\`))
	_, ok := apocConvertToJSON(func() {})
	require.False(t, ok, "a value JSON can't hold")
}
