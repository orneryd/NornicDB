package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPipelineRegistryCancellationAtEveryReachablePoll(t *testing.T) {
	items := make([]interface{}, 513)
	typed := make([]int, len(items))
	fields := make(map[string]interface{}, len(items))
	for i := range items {
		items[i] = int64(i)
		typed[i] = i
		fields[fmt.Sprint(i)] = i
	}
	for _, entry := range []struct {
		function string
		args     []string
	}{
		{"coll.distinct", []string{"items"}},
		{"coll.flatten", []string{"nested", "depth"}},
		{"coll.indexOf", []string{"items", "missing"}},
		{"coll.insert", []string{"items", "offset", "missing"}},
		{"coll.remove", []string{"items", "offset"}},
		{"coll.min", []string{"items"}},
		{"coll.max", []string{"items"}},
		{"coll.sort", []string{"items"}},
		{"keys", []string{"fields"}},
		{"keys", []string{"node"}},
		{"keys", []string{"edge"}},
		{"toUpper", []string{"text"}},
		{"toLower", []string{"text"}},
		{"upper", []string{"text"}},
		{"lower", []string{"text"}},
		{"normalize", []string{"text"}},
		{"char_length", []string{"text"}},
		{"size", []string{"text"}},
		{"labels", []string{"node"}},
		{"tail", []string{"items"}},
		{"toIntegerList", []string{"items"}},
		{"toFloatList", []string{"items"}},
		{"toBooleanList", []string{"items"}},
		{"toStringList", []string{"items"}},
		{"nullIf", []string{"items", "items"}},
	} {
		for _, list := range []interface{}{items, typed} {
			t.Run(entry.function+"/"+entry.args[0]+fmt.Sprintf("/%T", list), func(t *testing.T) {
				runPipelineCancelPointStage(t, func(ctx context.Context) error {
					probe := pipelineCancellation{ctx: ctx}
					values := map[string]interface{}{
						"items": list, "nested": []interface{}{list}, "depth": int64(1),
						"offset": int64(256), "missing": int64(-1), "fields": fields,
						"node": &storage.Node{Properties: fields, Labels: make([]string, 513)}, "edge": &storage.Edge{Properties: fields},
						"text": strings.Repeat("ΟΣßİé ", 1<<15),
					}
					_, found, err := cypherfn.EvaluateFunction(entry.function, entry.args, cypherfn.Context{
						Cancellation: &probe,
						Nodes:        map[string]*storage.Node{"node": values["node"].(*storage.Node)},
						Rels:         map[string]*storage.Edge{"edge": values["edge"].(*storage.Edge)},
						Eval:         func(name string) (interface{}, error) { return values[name], nil },
					})
					require.True(t, found)
					return err
				})
			})
		}
	}
}

func TestPipelineWideColumnsAndOrderTermsCancellation(t *testing.T) {
	row := pipelineRow{}
	for i := 0; i < 513; i++ {
		row[fmt.Sprintf("field%04d", i)] = int64(i)
	}
	t.Run("wildcard_columns", func(t *testing.T) {
		runPipelineCancelPointStage(t, func(ctx context.Context) error {
			_ = pipelineWildcardColumns(ctx, []pipelineRow{row})
			return getExpressionFailure(ctx)
		})
	})
	t.Run("order_terms", func(t *testing.T) {
		executor := NewStorageExecutor(newTestMemoryEngine(t))
		terms := make([]orderByTerm, 513)
		for i := range terms {
			terms[i] = orderByTerm{column: "tail"}
		}
		terms[0] = orderByTerm{column: "nested"}
		rows := []pipelineRow{{"nested": make([]interface{}, 513), "tail": int64(0)}, {"nested": make([]interface{}, 513), "tail": int64(0)}}
		runPipelineCancelPointStage(t, func(ctx context.Context) error {
			_ = executor.orderPipelineRowsWithScopes(ctx, rows, rows, terms)
			return getExpressionFailure(ctx)
		})
	})
}

func TestPipelineRegistryDispatcherCancellation(t *testing.T) {
	e := NewStorageExecutor(newTestMemoryEngine(t))
	items := make([]interface{}, 513)
	pairs := make([]interface{}, len(items))
	fields := map[string]interface{}{}
	for i := range items {
		items[i] = int64(i)
		pairs[i] = []interface{}{fmt.Sprint(i), int64(i)}
		fields[fmt.Sprint(i)] = i
	}
	row := pipelineRow{"items":items,"pairs":pairs,"fields":fields}
	for _, expression := range []string{
		"apoc.coll.pairs(items)", "apoc.coll.frequencies(items)",
				"apoc.map.merge(fields,fields)", "apoc.map.fromPairs(pairs)", "apoc.map.fromLists(items,items)",
		"apoc.map.removeKey(fields,'missing')", "apoc.map.setKey(fields,'extra',1)",
		"apoc.map.clean(fields,[],items)", "apoc.map.clean(fields,['missing'],[])",
	} {
		t.Run(expression,func(t *testing.T){
			runPipelineCancelPointStage(t,func(ctx context.Context)error{
				ctx = withPipelineCancellation(withValueBindings(ctx,row))
				_ = e.evaluateExpressionWithContextFullFunctions(ctx,expression,nil,nil,nil,nil,nil,0)
				return getExpressionFailure(ctx)
			})
		})
	}
	t.Run("compiled_registry",func(t *testing.T){
		runPipelineCancelPointStage(t,func(ctx context.Context)error{
			ctx = withPipelineCancellation(ctx)
			compiled,ok := e.tryCompileTraversalFunctionCall(ctx,"keys(n)")
			require.True(t,ok)
			_ = compiled(traversalOptRow{nodes:binding{"n":&storage.Node{Properties:fields}}})
			return getExpressionFailure(ctx)
		})
	})
}
