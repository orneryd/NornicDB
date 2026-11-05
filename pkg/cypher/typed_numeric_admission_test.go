package cypher

import (
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/testing/cypher/listinputs"
	"github.com/stretchr/testify/require"
)

// Exercise admission before the shared numeric/list readers, including a
// direct parameter-map path inside the native context evaluator. Runtime maps
// must not apply physical-property coercion to their numeric descendants.
func TestTypedNumericValueAdmission(t *testing.T) {
	exec, _ := typedNumericExecutor(t)
	for _, disabled := range []bool{false, true} {
		rowValueProgramsDisabled.Store(disabled)
		for _, input := range listinputs.NumericInputs() {
			t.Run(fmt.Sprintf("programsDisabled=%t/%s", disabled, input.Name), func(t *testing.T) {
				params := map[string]interface{}{"xs": input.List, "box": map[string]interface{}{"items": []interface{}{input.Scalar}}}
				for _, query := range []string{
					"RETURN toInteger(head($box.items))",
					"RETURN apoc.map.setKey({},'v',toInteger(head($box.items))).v",
					"WITH {items:[$xs]} AS box RETURN toInteger(head(head(box.items)))",
				} {
					require.Equal(t, input.Core, typedNumericRead(t, exec, query, params), query)
				}
			})
		}
	}
}

// Storage retains its existing Long/Double representation policy. Both uint
// carriers must use the checked converter before choosing the Double policy;
// overflow cannot turn into a negative Long. Expected values use Go kind
// accessors rather than any production conversion helper.
func TestTypedNumericPropertyAdmission(t *testing.T) {
	exec, _ := typedNumericExecutor(t)
	for _, input := range listinputs.NumericInputs() {
		t.Run(input.Name, func(t *testing.T) {
			value := reflect.ValueOf(input.Scalar)
			var want interface{}
			switch value.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				want = value.Int()
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				if value.Uint() > math.MaxInt64 {
					want = float64(value.Uint())
				} else {
					want = int64(value.Uint())
				}
			case reflect.Float32, reflect.Float64:
				want = value.Float()
			default:
				want = input.Scalar
			}
			query := "CREATE (n:NumericAdmission {v:$value}) RETURN n.v"
			got := typedNumericRead(t, exec, query, map[string]interface{}{"value": input.Scalar})
			require.Equal(t, typedNumericJSON(t, want), typedNumericJSON(t, got), query)
		})
	}
}
