package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// withPluginFunctions installs a plugin lookup over functions, keyed by name
// as registered.
func withPluginFunctions(t *testing.T, functions map[string]interface{}) {
	t.Helper()
	previous := PluginFunctionLookup
	PluginFunctionLookup = func(name string) (interface{}, bool) {
		handler, found := functions[name]
		return handler, found
	}
	t.Cleanup(func() { PluginFunctionLookup = previous })
}

// A plugin function is found by its name as written, case included, in the
// unknown-function check and in both evaluators; another spelling is an
// unknown function, as in Neo4j 5.26.30 with APOC (apoc.text.camelCase
// works, apoc.text.camelcase is Unknown function) (#1163).
func TestPluginFunctionsMatchTheirNameAsWritten(t *testing.T) {
	withPluginFunctions(t, map[string]interface{}{
		"zz.text.camelCase": func(text string) string { return "camel:" + text },
	})
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "plugin_name_case"))
	ctx := context.Background()

	result, err := exec.Execute(ctx, "RETURN zz.text.camelCase('a') AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"camel:a"}}, result.Rows)
	result, err = exec.Execute(ctx, "UNWIND ['a', 'b'] AS s WITH s WHERE zz.text.camelCase(s) = 'camel:a' RETURN s", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a"}}, result.Rows)
	for _, query := range []string{"RETURN zz.text.camelcase('a') AS v", "RETURN ZZ.TEXT.CAMELCASE('a') AS v"} {
		_, err := exec.Execute(ctx, query, nil)
		requireSemanticDetail(t, err, "UnknownFunction")
	}

	fixtureAdmission813Context, fixtureAdmission813 := expressionTestContext(ctx, "zz.text.camelCase('a')")
	_ = fixtureAdmission813
	require.Equal(t, "camel:a", exec.evaluateExpressionWithContext(fixtureAdmission813, fixtureAdmission813Context, "zz.text.camelCase('a')", nil, nil))
	fixtureAdmission814Context, fixtureAdmission814 := expressionTestRow(context.Background(), "zz.text.camelCase('a')", nil)
	_ = fixtureAdmission814
	value, ok, err := exec.evaluateRowValue(fixtureAdmission814, "zz.text.camelCase('a')", fixtureAdmission814Context)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "camel:a", value)
	// An argument the row evaluator can't resolve leaves the call unresolved.
	fixtureAdmission815Context, fixtureAdmission815 := expressionTestRow(context.Background(), "zz.text.camelCase(missing)", nil)
	_ = fixtureAdmission815
	_, ok, err = exec.evaluateRowValue(fixtureAdmission815, "zz.text.camelCase(missing)", fixtureAdmission815Context)
	require.NoError(t, err)
	require.False(t, ok)
}

// Both evaluators ask a plugin first: a plugin function overrides a built-in
// of the same name, and when the plugin's handler can't take the call the
// built-in answers instead. A plugin-only function the plugin can't call is
// the same error in both (#1163).
func TestPluginFunctionsComeFirstAndBuiltInsAnswerWhatPluginsCantCall(t *testing.T) {
	withPluginFunctions(t, map[string]interface{}{
		"apoc.text.join": func(chan int) int8 { return 0 },
		"apoc.coll.sum":  func(values []interface{}) float64 { return 999 },
		"zz.only":        func(chan int) int8 { return 0 },
	})
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "plugin_first"))
	ctx := context.Background()

	for expression, want := range map[string]interface{}{
		"apoc.text.join(['a', 'b'], '-')": "a-b",
		"apoc.coll.sum([1, 2, 3])":        999.0,
	} {
		result, err := exec.Execute(ctx, "RETURN "+expression+" AS v", nil)
		require.NoError(t, err, expression)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, expression)
		fixtureAdmission816Context, fixtureAdmission816 := expressionTestContext(ctx, expression)
		_ = fixtureAdmission816
		require.Equal(t, want, exec.evaluateExpressionWithContext(fixtureAdmission816, fixtureAdmission816Context, expression, nil, nil), expression)
		fixtureAdmission817Context, fixtureAdmission817 := expressionTestRow(context.Background(), expression, nil)
		_ = fixtureAdmission817
		value, ok, err := exec.evaluateRowValue(fixtureAdmission817, expression, fixtureAdmission817Context)
		require.NoError(t, err, expression)
		require.True(t, ok, expression)
		require.Equal(t, want, value, expression)
	}

	_, err := exec.Execute(ctx, "RETURN zz.only(1) AS v", nil)
	require.ErrorContains(t, err, "unsupported function signature")
	fixtureAdmission818Context, fixtureAdmission818 := expressionTestRow(context.Background(), "zz.only(1)", nil)
	_ = fixtureAdmission818
	_, _, rowErr := exec.evaluateRowValue(fixtureAdmission818, "zz.only(1)", fixtureAdmission818Context)
	require.ErrorContains(t, rowErr, "unsupported function signature")
	failureCtx := withExpressionFailureSlot(ctx)
	fixtureAdmission819Context, fixtureAdmission819 := expressionTestContext(failureCtx, "zz.only(1)")
	_ = fixtureAdmission819
	require.Nil(t, exec.evaluateExpressionWithContext(fixtureAdmission819, fixtureAdmission819Context, "zz.only(1)", nil, nil))
	require.Equal(t, rowErr, getExpressionFailure(failureCtx))
}
