package cypher

import (
	"context"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/listinputs"
	"github.com/stretchr/testify/require"
)

func typedNumericWidths() []listinputs.Input {
	var result []listinputs.Input
	for _, input := range listinputs.Inputs() {
		switch input.Name {
		case "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64", "float32", "float64", "array", "named-ints":
			result = append(result, input)
		}
	}
	originals := append([]listinputs.Input(nil), result...)
	for _, input := range originals {
		original := reflect.ValueOf(input.Value)
		kind := original.Type().Elem().Kind()
		if kind != reflect.Int && kind != reflect.Int8 && kind != reflect.Int16 && kind != reflect.Int32 && kind != reflect.Int64 && kind != reflect.Float32 && kind != reflect.Float64 {
			continue
		}
		var negative reflect.Value
		if original.Kind() == reflect.Array {
			negative = reflect.New(original.Type()).Elem()
			negative.Set(original)
		} else {
			negative = reflect.MakeSlice(original.Type(), original.Len(), original.Len())
			reflect.Copy(negative, original)
		}
		if kind == reflect.Float32 || kind == reflect.Float64 {
			negative.Index(1).SetFloat(-negative.Index(1).Float())
		} else {
			negative.Index(1).SetInt(-negative.Index(1).Int())
		}
		input.Name += "-negative"
		input.Value = negative.Interface()
		input.Literal = strings.Replace(input.Literal, ",1", ",-1", 1)
		result = append(result, input)
	}
	return result
}

func TestTypedNumericVectorProcedures(t *testing.T) {
	exec, store := typedNumericExecutor(t)
	ctx := context.Background()
	_, err := store.CreateNode(&storage.Node{ID: "a", Labels: []string{"NumericVector"}, Properties: map[string]interface{}{"content": "alpha", "xs": []float64{3, 1, 3}}, ChunkEmbeddings: [][]float32{{3, 1, 3}}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "b", Labels: []string{"Endpoint"}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "NUMERIC_VECTOR", Properties: map[string]interface{}{"xs": []float64{3, 1, 3}}}))
	service := search.NewService(store, 3, nil)
	service.FixVectorDimensions()
	exec.SetSearchService(service)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for _, query := range []string{
		"CREATE VECTOR INDEX numeric_nodes FOR (n:NumericVector) ON (n.xs) OPTIONS {indexConfig:{`vector.dimensions`:3}}",
		"CREATE VECTOR INDEX numeric_edges FOR ()-[r:NUMERIC_VECTOR]-() ON (r.xs) OPTIONS {indexConfig:{`vector.dimensions`:3}}",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
	require.NoError(t, service.BuildIndexes(ctx))
	for _, disabled := range []bool{false, true} {
		rowValueProgramsDisabled.Store(disabled)
		for _, input := range typedNumericWidths() {
			t.Run(input.Name+boolTestName(disabled), func(t *testing.T) {
				for _, procedure := range []struct{ name, index string }{{"queryNodes", "numeric_nodes"}, {"queryRelationships", "numeric_edges"}} {
					suffix := ") YIELD score RETURN score AS v"
					prefix := "CALL db.index.vector." + procedure.name + "('" + procedure.index + "',1,"
					want := typedNumericRead(t, exec, prefix+input.Literal+suffix, nil)
					for _, form := range []struct{ prefix, value string }{{"", "$xs"}, {"WITH $xs AS xs ", "xs"}, {"MATCH (n:NumericVector) ", "n.xs"}} {
						params := map[string]interface{}{"xs": input.Value}
						expected := want
						if form.value == "n.xs" {
							expected = typedNumericRead(t, exec, prefix+"[3,1,3]"+suffix, nil)
						}
						require.Equal(t, expected, typedNumericRead(t, exec, form.prefix+prefix+form.value+suffix, params))
					}
				}
				// RAG has strict and permissive policies, both of which visit the same
				// numeric/list readers. The actual indexed document proves the input was
				// used, rather than merely accepting a request against an empty service.
				for _, failClosed := range []bool{false, true} {
					request := map[string]interface{}{"query": "alpha", "embedding": input.Value, "failClosed": failClosed, "limit": 1}
					got := typedNumericRead(t, exec, "CALL db.retrieve($request) YIELD node RETURN node.content AS v", map[string]interface{}{"request": request})
					require.Equal(t, "alpha", got)
				}
			})
		}
	}
	for _, value := range []interface{}{[]float64{1e100, 1, 3}, []interface{}{1e100, 1, 3}, []string{"3", "1", "3"}} {
		for _, procedure := range []string{"queryNodes('numeric_nodes'", "queryRelationships('numeric_edges'"} {
			_, err := exec.Execute(ctx, "CALL db.index.vector."+procedure+",1,$xs)", map[string]interface{}{"xs": value})
			require.Error(t, err)
		}
		_, err := exec.Execute(ctx, "CALL db.retrieve($request)", map[string]interface{}{"request": map[string]interface{}{"query": "alpha", "embedding": value, "failClosed": true}})
		require.Error(t, err)
	}
}

func boolTestName(disabled bool) string {
	if disabled {
		return "/programsDisabled=true"
	}
	return "/programsDisabled=false"
}

func TestTypedNumericGDSProjection(t *testing.T) {
	previous := graphProjections
	graphProjections = make(map[string]*GraphProjection)
	t.Cleanup(func() { graphProjections = previous; rowValueProgramsDisabled.Store(false) })
	for _, input := range typedNumericWidths() {
		t.Run(input.Name, func(t *testing.T) {
			// Unsupported physical property-array widths retain their numeric values
			// inside the existing boxed property carrier. Supported typed arrays stay
			// typed; storage's set of physical property types is unchanged.
			property := input.PropertyValue()
			switch input.Name {
			case "int8", "int16", "uint", "uint16", "uint32", "uint64":
				source := reflect.ValueOf(input.Value)
				boxed := make([]interface{}, source.Len())
				for i := range boxed {
					boxed[i] = source.Index(i).Interface()
				}
				property = boxed
			}
			exec, run := newGDSEngineGraph(t, "numeric_gds_"+input.Name, gdsNode("a", "N", map[string]interface{}{"xs": property}))
			_, err := run("CALL gds.graph.project('numeric', {N:{properties:['xs']}}, '*')")
			require.NoError(t, err)
			for _, disabled := range []bool{false, true} {
				rowValueProgramsDisabled.Store(disabled)
				want := typedNumericRead(t, exec, "RETURN "+input.Literal+" AS v", nil)
				got := typedNumericRead(t, exec, "MATCH (n:N) RETURN gds.util.nodeProperty('numeric',n,'xs') AS v", nil)
				require.Equal(t, typedNumericJSON(t, want), typedNumericJSON(t, got))
			}
			_, err = run("CALL gds.graph.drop('numeric')")
			require.NoError(t, err)
		})
	}
	for _, value := range []interface{}{uint64(math.MaxUint64), []interface{}{uint64(math.MaxUint64)}} {
		_, run := newGDSEngineGraph(t, "numeric_gds_overflow", gdsNode("a", "N", map[string]interface{}{"xs": value}))
		_, err := run("CALL gds.graph.project('overflow', {N:{properties:['xs']}}, '*')")
		require.Error(t, err, "unrepresentable Longs must not become stored zeroes")
	}
}
