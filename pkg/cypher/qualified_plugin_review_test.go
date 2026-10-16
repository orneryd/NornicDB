package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestQualifiedPluginReviewNames(t *testing.T) {
	registerQualifiedReviewHandlers(t)
	for _, suffix := range []string{"range", "truncate", "shortestPath", "allShortestPaths", "and", "or", "xor"} {
		for _, name := range []string{"custom." + suffix, "custom . " + suffix} {
			t.Run(name, func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "qualified_review"))
				for _, value := range []interface{}{"ok", "", nil, strings.Repeat("x", 65536)} {
					result, err := exec.Execute(context.Background(), "RETURN "+name+"($value) AS v", map[string]interface{}{"value": value})
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{value}}, result.Rows)
				}
				result, err := exec.Execute(context.Background(), "RETURN "+name+"('ok') AS v", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{"ok"}}, result.Rows)
			})
		}
	}
}

func registerQualifiedReviewHandlers(t *testing.T) {
	t.Helper()
	previous := PluginFunctionLookup
	t.Cleanup(func() { PluginFunctionLookup = previous })
	PluginFunctionLookup = withPurePluginContracts(func(name string) (interface{}, bool) {
		switch name {
		case "custom.range", "custom.truncate", "custom.shortestPath", "custom.allShortestPaths", "custom.and", "custom.or", "custom.xor":
			return func(v interface{}) interface{} { return v }, true
		}
		return nil, false
	})
}

func TestQualifiedPluginReviewStreamsAndTransactions(t *testing.T) {
	registerQualifiedReviewHandlers(t)
	for _, mode := range []string{"auto", "explicit"} {
		for _, suffix := range []string{"range", "truncate", "shortestPath", "allShortestPaths", "and", "or", "xor"} {
			t.Run(mode+"/"+suffix, func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "plugin_streams"))
				ctx := context.Background()
				if mode == "explicit" {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
				}
				name := "custom . " + suffix
				result, err := exec.Execute(ctx, "UNWIND range(1,10000) AS x RETURN count("+name+"(x)) AS n", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(10000)}}, result.Rows)
				result, err = exec.Execute(ctx, "UNWIND [] AS x RETURN "+name+"(x) AS v", nil)
				require.NoError(t, err)
				require.Empty(t, result.Rows)
				result, err = exec.Execute(ctx, "UNWIND [false,true] AS x WITH x WHERE "+name+"(x) RETURN x", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{true}}, result.Rows)
			})
		}
	}
}

func TestQualifiedReviewUnregisteredBooleanNames(t *testing.T) {
	previous := PluginFunctionLookup
	PluginFunctionLookup = nil
	t.Cleanup(func() { PluginFunctionLookup = previous })
	for _, name := range []string{"custom.and", "custom . or", "custom . xor", "custom . range", "custom . shortestPath"} {
		t.Run(name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "plugin_unknown"))
			_, err := exec.Execute(context.Background(), "RETURN "+name+"('ok') AS v", nil)
			require.ErrorContains(t, err, "Neo.ClientError.Statement.SyntaxError: unknown function: "+strings.ReplaceAll(name, " ", ""))
		})
	}
}

func TestReviewedCallableNameBoundaries(t *testing.T) {
	for _, test := range []struct {
		source, want string
		ok           bool
	}{
		{"range", "range", true}, {"custom.range", "custom.range", true}, {"custom.path.value", "custom.path.value", true},
		{" custom . range ", "custom.range", true}, {"custom/*gap*/.range", "custom.range", true},
		{"`custom` . `range`", "custom.range", true}, {"custom.`a``b`", "custom.a`b", true}, {"custom.λ", "custom.λ", true},
		{"", "", false}, {"0bad.range", "", false}, {"custom..range", "", false}, {"custom.0bad", "", false}, {"custom.range.", "", false}, {"custom.range trailing", "", false}, {"custom.`unclosed", "", false},
	} {
		t.Run(test.source, func(t *testing.T) {
			name, ok := canonicalCallableName(test.source)
			require.Equal(t, test.ok, ok)
			require.Equal(t, test.want, name)
		})
	}
}
