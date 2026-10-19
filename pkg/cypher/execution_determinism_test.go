package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The population is the function catalog crossed with the procedure registry's
// executable-input declarations, rather than a timestamp-specific fixture.
func TestExecutionDeterminismRegisteredInputs(t *testing.T) {
	exec := solR2CacheExecutor(t)
	ensureBuiltInProceduresRegistered()
	seen := map[string]bool{}
	functions, inputs := 0, 0
	for _, function := range cypherFunctionCatalog {
		if function.cacheVolatility == functionStable || seen[function.name] {
			continue
		}
		seen[function.name] = true
		functions++
		invocation := function.name + "()"
		t.Run(function.name+"/outer", func(t *testing.T) {
			require.False(t, isCacheableReadQuery("RETURN "+invocation+" AS value"))
		})
		for _, procedure := range globalProcedureRegistry.ListBuiltIns() {
			for index, input := range procedure.ResultCache.CypherInputs {
				if input.StoredText != "" { continue }
				inputs++
				t.Run(fmt.Sprintf("%s/%s/%d", function.name, procedure.Name, index), func(t *testing.T) {
					args := make([]string, procedure.MinArgs)
					if len(args) <= input.Argument { args = make([]string, input.Argument+1) }
					params := map[string]any{}
					for i := range args { args[i] = "{}" }
					text := "RETURN " + invocation + " AS value"
					if input.Field != "" {
						text = invocation + " IS NOT NULL"
						params["request"] = map[string]any{"query":"transcript", input.Field:text}
						args[input.Argument] = "$request"
					} else {
						params["statement"] = text
						args[input.Argument] = "$statement"
					}
					query := "CALL " + procedure.Name + "(" + strings.Join(args, ",") + ")"
					ctx := withQueryParams(context.Background(), params)
					require.False(t, exec.canUseResultCache(ctx, &QueryInfo{IsReadOnly:true}, query), query)
				})
			}
		}
	}
	require.GreaterOrEqual(t, functions, 12, "include aliases and UUID, not only the original six spellings")
	require.Greater(t, inputs, functions, "exercise every declared executable argument")
}

func TestExecutionDeterminismPublicPredicates(t *testing.T) {
	exec := solR2CacheExecutor(t)
	seen := map[string]bool{}
	for _, function := range cypherFunctionCatalog {
		if function.cacheVolatility == functionStable || strings.HasPrefix(function.name,"gds.") || strings.HasPrefix(function.name,"graph.") || seen[function.name] { continue }
		seen[function.name] = true
		t.Run(function.name, func(t *testing.T) {
			for _, query := range []string{"RETURN "+function.name+"() AS value", "CALL db.rretrieve($request)"} {
				params := map[string]any{"request":map[string]any{"query":"transcript","where":function.name+"() IS NOT NULL"}}
				first, err := exec.Execute(context.Background(), query, params)
				require.NoError(t, err, query)
				second, err := exec.Execute(context.Background(), query, params)
				require.NoError(t, err, query)
				require.NotSame(t, first, second, "outer and parameterized predicates must execute: %s",query)
			}
		})
	}
}
