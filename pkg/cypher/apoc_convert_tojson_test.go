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
	require.Equal(t, `["a<b&c>"]`, exec.evaluateExpressionWithContext(ctx, "apoc.convert.toJson(['a<b&c>'])", nil, nil))
	_, ok := apocConvertToJSON(func() {})
	require.False(t, ok, "a value JSON can't hold")
}
