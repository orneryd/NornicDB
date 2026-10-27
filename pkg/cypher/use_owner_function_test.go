package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUseOwnerFunctionInventory(t *testing.T) {
	for _, selector := range []string{"USE ", "USE GRAPH "} {
		for _, test := range []struct {
			reference string
			want      []string
		}{
			{"graph.byName('cmp.a')", nil},
			{"graph /* name */ . byName(toLower('CMP.A'))", []string{"tolower"}},
			{"graph.byName(toString(rand()))", []string{"tostring", "rand"}},
			{"graph.byElementId(elementId(n))", []string{"elementid"}},
			{"`graph`.`byName`(toLower('CMP.A'))", []string{"tolower"}},
			{"graph", nil},
			{"match", nil},
		} {
			t.Run(selector+test.reference, func(t *testing.T) {
				query := selector + test.reference + " RETURN abs(-1) AS n"
				var got []string
				require.NoError(t, forEachFunctionInvocation(query, func(name, normalized string, open int) error {
					require.Equal(t, byte('('), query[open])
					// Invocation coordinates still address the original source.
					require.True(t, strings.HasSuffix(strings.ToLower(strings.TrimSpace(query[:open])), normalized), name)
					got = append(got, normalized)
					return nil
				}))
				require.Equal(t, append(append([]string{}, test.want...), "abs"), got)
				require.NoError(t, validateKnownFunctions(query))
			})
		}
		query := "CALL { " + selector + "graph.byName(toLower('CMP.A')) RETURN abs(-1) AS n } RETURN n"
		var got []string
		require.NoError(t, forEachFunctionCall(query, func(_, normalized string) error {
			got = append(got, normalized)
			return nil
		}))
		require.Equal(t, []string{"tolower", "abs"}, got)
		requireSemanticDetail(t, validateKnownFunctions(selector+"graph.byName(noSuchSelectorArgument()) RETURN 1"), "UnknownFunction")
	}
	var got []string
	require.NoError(t, forEachFunctionCall("CALL db.labels(toString(1)) YIELD label RETURN size(label)", func(_, normalized string) error {
		got = append(got, normalized)
		return nil
	}))
	require.Equal(t, []string{"tostring", "size"}, got, "CALL retains its separate procedure-name handling")
	for _, query := range []string{"RETURN n.use, abs(-1)", "RETURN {use: abs(-1)}", "WITH 1 AS use RETURN abs(use)"} {
		got = nil
		require.NoError(t, forEachFunctionCall(query, func(_, normalized string) error {
			got = append(got, normalized)
			return nil
		}))
		require.Equal(t, []string{"abs"}, got, query)
	}
}

func TestUseOwnerFunctionCacheability(t *testing.T) {
	exec := solR2CacheExecutor(t)
	for _, selector := range []string{"USE ", "USE GRAPH "} {
		for _, test := range []struct {
			reference string
			cacheable bool
		}{
			{"graph.byName('cmp.a')", true},
			{"graph.byName(toLower('CMP.A'))", true},
			{"graph.byName(toString(rand()))", false},
		} {
			query := selector + test.reference + " RETURN 1 AS n"
			t.Run(query, func(t *testing.T) {
				ctx := context.Background()
				require.Equal(t, test.cacheable, compileExecutionPolicy(ctx, query, 0).cacheable)
				policy := cachedExecutionPolicy(ctx, query)
				require.Equal(t, test.cacheable, policy.cacheable)
				require.Same(t, policy, cachedExecutionPolicy(ctx, query), "compiled admission cache hit")
				require.Equal(t, test.cacheable, exec.canUseResultCache(ctx, &QueryInfo{IsReadOnly: true}, query))
			})
		}
	}
}
