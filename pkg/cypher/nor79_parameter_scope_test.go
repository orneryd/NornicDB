package cypher

import (
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

// One executor and query text must recheck each bound value independently of
// cached compilation, including the same caller-owned map reused across calls.
func TestNOR79ParameterScopeRebinding(t *testing.T) {
	queries := []string{
		"UNWIND [] AS r WITH $p AS v RETURN toUpper(v) AS a",
		"UNWIND [] AS r CALL () { WITH $p AS v RETURN toUpper(v) AS a } RETURN a",
		"UNWIND [] AS r WITH $p AS v RETURN toUpper(v) AS a UNION ALL UNWIND [] AS r WITH $p AS v RETURN toUpper(v) AS a",
	}
	forEachParser(t, func(t *testing.T) {
		for _, query := range queries {
			for _, prefix := range []string{"", "EXPLAIN ", "PROFILE "} {
				t.Run(prefix+query, func(t *testing.T) {
					exec := norStaticFreshExecutor(t)
					exec.cache = nil
					params := map[string]interface{}{}
					for _, value := range []interface{}{"first", int64(1), "second", 1.5, int64(2), "last"} {
						params["p"] = value
						result, err := exec.Execute(t.Context(), prefix+query, params)
						if _, rejects := value.(int64); rejects {
							require.Error(t, err, "%T", value)
							code, message := nornicerrors.Neo4jStatus(err)
							require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
							require.Contains(t, message, "Type mismatch")
						} else {
							require.NoError(t, err, "%T", value)
							require.NotNil(t, result)
							if prefix == "" {
								require.Empty(t, result.Rows)
							}
						}
					}
				})
			}
		}
	})
}

// A request parameter may prove an outer value without leaking that proof into
// a later rebinding, comprehension variable, or independently scoped CALL.
func TestNOR79ParameterScopeShadowing(t *testing.T) {
	queries := []string{
		"UNWIND [] AS r WITH $p AS v WITH 'local' AS v RETURN toUpper(v) AS a",
		"UNWIND [] AS r WITH $p AS v RETURN [v IN ['local'] | toUpper(v)] AS a",
		"UNWIND [] AS r WITH $p AS v CALL () { WITH 'local' AS v RETURN toUpper(v) AS a } RETURN a",
	}
	forEachParser(t, func(t *testing.T) {
		for _, query := range queries {
			for _, prefix := range []string{"", "EXPLAIN ", "PROFILE "} {
				t.Run(prefix+query, func(t *testing.T) {
					exec := norStaticFreshExecutor(t)
					result, err := exec.Execute(t.Context(), prefix+query, map[string]interface{}{"p": int64(1)})
					require.NoError(t, err)
					require.NotNil(t, result)
					if prefix == "" {
						require.Empty(t, result.Rows)
					}
				})
			}
		}
	})
}
