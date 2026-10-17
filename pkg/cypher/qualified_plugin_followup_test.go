package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestQualifiedPluginIdentityWithSpacesAndOperatorNames(t *testing.T) {
	previous := PluginFunctionLookup
	t.Cleanup(func() { PluginFunctionLookup = previous })
	names := []string{"custom.range", "custom.truncate", "custom.shortestPath", "custom.allShortestPaths", "custom.and", "custom.or", "custom.xor"}
	PluginFunctionLookup = withPurePluginContracts(func(name string) (interface{}, bool) {
		for _, registered := range names {
			if name == registered {
				return func(value interface{}) interface{} { return value }, true
			}
		}
		return nil, false
	})
	for _, explicit := range []bool{false, true} {
		mode := map[bool]string{false: "auto", true: "explicit"}[explicit]
		for _, name := range names {
			for _, separator := range []string{".", " . ", ". ", " .", "\t.\n"} {
				call := strings.ReplaceAll(name, ".", separator)
				t.Run(mode+"/"+call, func(t *testing.T) {
					ctx := context.Background()
					executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "qualified_followup"))
					if explicit {
						_, err := executor.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						t.Cleanup(func() { _, _ = executor.Execute(ctx, "ROLLBACK", nil) })
					}
					for _, sample := range []struct {
						expression string
						value      interface{}
					}{
						{"'ok'", "ok"}, {"''", ""}, {"null", nil}, {"$large", strings.Repeat("x", 65536)},
					} {
						result, err := executor.Execute(ctx, "RETURN "+call+"("+sample.expression+") AS v", map[string]interface{}{"large": sample.value})
						require.NoError(t, err, sample.expression)
						require.Equal(t, [][]interface{}{{sample.value}}, result.Rows, sample.expression)
					}
					for _, sample := range []struct {
						query string
						rows  [][]interface{}
					}{
						{"UNWIND [] AS x RETURN " + call + "(x) AS v", nil},
						{"UNWIND range(1,10000) AS x RETURN count(" + call + "(x)) AS n", [][]interface{}{{int64(10000)}}},
						{"UNWIND ['ok','no'] AS x WITH x WHERE " + call + "(x) = 'ok' RETURN x", [][]interface{}{{"ok"}}},
					} {
						result, err := executor.Execute(ctx, sample.query, nil)
						require.NoError(t, err, sample.query)
						if sample.rows == nil {
							require.Empty(t, result.Rows)
						} else {
							require.Equal(t, sample.rows, result.Rows, sample.query)
						}
					}
					if explicit {
						_, err := executor.Execute(ctx, "COMMIT", nil)
						require.NoError(t, err)
					}
				})
			}
		}
	}
}

func TestQualifiedPluginFollowupUnknownAndNativeControls(t *testing.T) {
	previous := PluginFunctionLookup
	PluginFunctionLookup = nil
	t.Cleanup(func() { PluginFunctionLookup = previous })
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "qualified_controls"))
	ctx := context.Background()
	for _, name := range []string{"range", "truncate", "shortestPath", "allShortestPaths", "and", "or", "xor"} {
		for _, separator := range []string{".", " . "} {
			_, err := executor.Execute(ctx, "RETURN custom"+separator+name+"('ok') AS v", nil)
			requireSemanticDetail(t, err, "UnknownFunction")
			require.ErrorContains(t, err, "custom."+name)
		}
	}
	for _, sample := range []struct {
		query string
		value interface{}
	}{
		{"RETURN range(1,3) AS v", []interface{}{int64(1), int64(2), int64(3)}},
		{"RETURN range(3,1) AS v", []interface{}{}},
		{"RETURN toString(date.truncate('month',date('2026-10-15'))) AS v", "2026-10-01"},
		{"RETURN true AND false OR true XOR false AS v", true},
	} {
		result, err := executor.Execute(ctx, sample.query, nil)
		require.NoError(t, err, sample.query)
		require.Equal(t, [][]interface{}{{sample.value}}, result.Rows, sample.query)
	}
	_, err := executor.Execute(ctx, "RETURN range(null,3)", nil)
	require.ErrorContains(t, err, "TypeError")
	_, err = executor.Execute(ctx, "RETURN true AND 'ok'", nil)
	require.ErrorContains(t, err, "expected Boolean")
	_, err = executor.Execute(ctx, "CREATE (a:Route {id:1})-[:R]->(b:Route {id:2})", nil)
	require.NoError(t, err)
	for _, name := range []string{"shortestPath", "allShortestPaths"} {
		result, err := executor.Execute(ctx, "MATCH (a:Route {id:1}), (b:Route {id:2}) MATCH p = "+name+"((a)-[:R*]->(b)) RETURN length(p) AS v", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	}
}

func TestQualifiedIdentifierReader(t *testing.T) {
	previous := PluginFunctionLookup
	t.Cleanup(func() { PluginFunctionLookup = previous })
	PluginFunctionLookup = withPurePluginContracts(func(name string) (interface{}, bool) {
		require.Equal(t, "custom.deep.And", name)
		return func(value interface{}) interface{} { return value }, true
	})
	decoded, _, ok := parseFunctionCallWS(" custom . deep . And ('ok') ")
	require.True(t, ok)
	_, found := pluginFunctionHandler(decoded)
	require.True(t, found)
	for _, sample := range []struct {
		text, name string
		valid      bool
	}{
		{"custom . range", "custom.range", true},
		{"custom\t.\nshortestPath", "custom.shortestPath", true},
		{"custom . deep . and", "custom.deep.and", true},
		{"`custom` . `and`", "custom.and", true},
		{"range", "range", true},
		{"", "", false},
		{"1custom.range", "", false},
		{"custom..range", "", false},
		{"custom .", "", false},
		{"custom range", "", false},
	} {
		name, inner, ok := parseFunctionCallWS(sample.text + "('ok')")
		require.Equal(t, sample.valid, ok, sample.text)
		if ok {
			require.Equal(t, sample.name, name)
			require.Equal(t, "'ok'", inner)
		}
		require.Equal(t, sample.valid, looksLikeFunctionCall(sample.text+"('ok')"), sample.text)
	}
	for _, keyword := range []string{"AND", "OR", "XOR"} {
		for _, expression := range []string{"custom." + keyword + "('ok')", "custom . " + keyword + "('ok')", keyword + " . custom('ok')", "n . " + keyword} {
			require.Equal(t, -1, infixKeywordIndex(expression, keyword), expression)
		}
		expression := "custom . " + keyword + "(true) " + keyword + " false"
		require.Equal(t, strings.LastIndex(expression, keyword), infixKeywordIndex(expression, keyword))
		require.Equal(t, len("true "), infixKeywordIndex("true "+keyword+" .5", keyword))
	}
	for _, text := range []string{"custom.range", "custom . range", "custom\t.\nrange", "myrange"} {
		require.False(t, functionNameStartsAt(text, strings.Index(text, "range")), text)
	}
	require.True(t, functionNameStartsAt("RETURN range", len("RETURN ")))
	require.True(t, functionNameStartsAt("range", 0))
}
