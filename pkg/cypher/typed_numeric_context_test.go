package cypher

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/listinputs"
	"github.com/stretchr/testify/require"
)

// Use the stock engine independently of the installed APOC library handlers:
// those handlers have a different argument-evaluation route and must not mask
// the legacy context reader inside the engine's own apoc.map.setKey function.
func TestTypedNumericEngineContextRepros(t *testing.T) {
	withPluginFunctions(t, nil)
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "numeric_engine_context")
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	t.Cleanup(func() { rowValueProgramsDisabled.Store(false) })
	for _, disabled := range []bool{false, true} {
		rowValueProgramsDisabled.Store(disabled)
		for _, input := range listinputs.NumericInputs() {
			t.Run(fmt.Sprintf("programsDisabled=%t/%s", disabled, input.Name), func(t *testing.T) {
				if !disabled {
					_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(input.Name), Labels: []string{"NumericContext"}, Properties: map[string]interface{}{"name": input.Name, "xs": input.Property}})
					require.NoError(t, err)
				}
				value := input.Scalar
				if reflected := reflect.ValueOf(value); reflected.Kind() == reflect.Float32 {
					value = reflected.Float()
				}
				params := map[string]interface{}{"xs": input.List, "name": input.Name}
				for _, form := range []struct{ name, prefix, list string }{
					{"parameter", "", "$xs"},
					{"with", "WITH $xs AS xs ", "xs"},
					{"stored", "MATCH (n:NumericContext {name:$name}) ", "n.xs"},
					{"nested", "WITH {items:[$xs]} AS box ", "head(box.items)"},
				} {
					for _, check := range []struct {
						expression string
						want       interface{}
					}{
						{"apoc.map.setKey({},'v',coalesce(%s,[])[0])", map[string]interface{}{"v": value}},
						{"apoc.map.setKey({},'v',coalesce(%s,[])[-1..])", map[string]interface{}{"v": []interface{}{value}}},
						{"apoc.map.setKey({},'v',toInteger(coalesce(%s,[])[0]))", map[string]interface{}{"v": input.Core}},
					} {
						t.Run(form.name+"/"+check.expression, func(t *testing.T) {
							query := form.prefix + listinputs.Query(check.expression, form.list)
							got := typedNumericRead(t, exec, query, params)
							require.Equal(t, typedNumericJSON(t, check.want), typedNumericJSON(t, got), query)
						})
					}
				}
			})
		}
	}
}

// Release retains its inherited integral-float context index extension;
// develop already required an integer. List recognition is shared in both.
const typedNumericIntegralFloatContextIndex = true

func TestTypedNumericContextSubscriptPolicies(t *testing.T) {
	withPluginFunctions(t, nil)
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "numeric_context_policy")
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	t.Cleanup(func() { rowValueProgramsDisabled.Store(false) })
	for _, disabled := range []bool{false, true} {
		rowValueProgramsDisabled.Store(disabled)
		for _, check := range []struct {
			query string
			want  interface{}
			code  string
		}{
			{"RETURN 'a\u00e9\U0001f642'[1] AS v", nil, "Neo.ClientError.Statement.SyntaxError"},
			{"RETURN apoc.map.setKey({},'v',coalesce($text,'')[1]) AS v", nil, "Neo.ClientError.Statement.TypeError"},
			{"RETURN apoc.map.setKey({},'v',{a:7}['a']) AS v", map[string]interface{}{"v": int64(7)}, ""},
			{"RETURN apoc.map.setKey({},'v',{a:7}[0]) AS v", nil, "Neo.ClientError.Statement.SyntaxError"},
			{"RETURN apoc.map.setKey({},'v',coalesce($xs,[])['1']) AS v", nil, "Neo.ClientError.Statement.TypeError"},
		} {
			t.Run(fmt.Sprintf("programsDisabled=%t/%s", disabled, check.query), func(t *testing.T) {
				got, err := exec.Execute(context.Background(), check.query, map[string]interface{}{"text": "a\u00e9\U0001f642", "xs": []int64{3, 1, 3}})
				if check.code != "" {
					requireStatusCode(t, err, check.code)
					return
				}
				require.NoError(t, err)
				require.Equal(t, typedNumericJSON(t, check.want), typedNumericJSON(t, got.Rows[0][0]))
			})
		}
		for _, index := range []interface{}{float32(1), float64(1)} {
			query := "RETURN apoc.map.setKey({},'v',coalesce($xs,[])[$index]) AS v"
			got, err := exec.Execute(context.Background(), query, map[string]interface{}{"xs": []int64{3, 1, 3}, "index": index})
			if !typedNumericIntegralFloatContextIndex {
				requireStatusCode(t, err, "Neo.ClientError.Statement.TypeError")
				continue
			}
			require.NoError(t, err)
			require.Equal(t, typedNumericJSON(t, map[string]interface{}{"v": int64(1)}), typedNumericJSON(t, got.Rows[0][0]))
		}
	}
}
