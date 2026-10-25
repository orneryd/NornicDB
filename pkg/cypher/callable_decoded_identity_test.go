package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCallableDecodedIdentity(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		t.Run(fmt.Sprint("normalize=", enabled), func(t *testing.T) {
			restore := config.WithCypherQueryNormalizationDisabled()
			defer restore()
			config.SetCypherQueryNormalizationEnabled(enabled)
			testCallableDecodedIdentity(t)
		})
	}
}
func testCallableDecodedIdentity(t *testing.T) {
	withPluginFunctions(t, map[string]interface{}{
		"custom.a b":          func(s string) string { return "space:" + s },
		"custom.a/*literal*/": func(s string) string { return "literal:" + s },
		"custom.a":            func(s string) string { return "wrong:" + s },
		"custom.range":        func(s string) string { return "range:" + s },
		"custom.and":          func(s string) string { return "and:" + s },
		"custom.or":           func(s string) string { return "or:" + s },
		"custom.xor":          func(s string) string { return "xor:" + s },
	})
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "decoded_identity"))
	for _, tc := range []struct{ expr, want string }{
		{"custom.`a b`('ok')", "space:ok"},
		{"custom.`a/*literal*/`('ok')", "literal:ok"},
		{"`custom` . range('ok')", "range:ok"},
		{"custom . range('ok')", "range:ok"},
		{"custom . /*c*/ range('ok')", "range:ok"},
		{"custom . //c\n range('ok')", "range:ok"},
		{"custom.and('ok')", "and:ok"}, {"custom.or('ok')", "or:ok"}, {"custom.xor('ok')", "xor:ok"},
	} {
		t.Run(tc.expr, func(t *testing.T) {
			r, err := e.Execute(context.Background(), "RETURN "+tc.expr+" AS v", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{tc.want}}, r.Rows)
			require.Equal(t, tc.want, e.evaluateExpressionWithContext(context.Background(), tc.expr, nil, nil))
			v, ok, err := e.evaluateRowValue(tc.expr, nil)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, tc.want, v)
		})
	}
}

func TestCommentedQualifiedNativeBoundary(t *testing.T) {
	text := "custom . /*c*/ range('ok')"
	require.False(t, functionNameStartsAt(text, len("custom . /*c*/ ")))
	require.Empty(t, namedFunctionArguments(text, "range"))
}

func TestQuotedQualifiedUnknownFunction(t *testing.T) {
	previous := PluginFunctionLookup
	PluginFunctionLookup = nil
	t.Cleanup(func() { PluginFunctionLookup = previous })
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "unknown_quoted"))
	_, err := e.Execute(context.Background(), "RETURN `custom` . range('ok') AS v", nil)
	requireSemanticDetail(t, err, "UnknownFunction")
}

func TestQuotedNamespaceStaticArguments(t *testing.T) {
	err := forEachStaticFunctionArgument(t.Context(), "`custom` . range('ok')", func(staticArgumentType, string, int) error { return nil })
	require.NoError(t, err)
	require.Empty(t, semanticExpressionReferences("custom . /*c*/ range('ok')"))
}
