package cypher

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/testing/cypher/listinputs"
	"github.com/stretchr/testify/require"
)

// This extension exists only on develop. Serialization keeps its byte-array
// policy, while every numeric coordinate and ordinary list uses common readers.
func TestTypedNumericJSONProfiles(t *testing.T) {
	exec, _ := typedNumericExecutor(t)
	for _, disabled := range []bool{false, true} {
		rowValueProgramsDisabled.Store(disabled)
		for _, profile := range []string{"python-canonical", "python-sorted"} {
			for _, input := range typedNumericWidths() {
				if input.Name == "uint8" {
					continue
				} // Python-driver profiles intentionally treat byte arrays as binary.
				query := func(value string) string {
					return "RETURN nornicdb.json.stringify(" + value + ",{profile:'" + profile + "'}) AS v"
				}
				want := typedNumericRead(t, exec, query(input.Literal), nil)
				for _, form := range []struct{ prefix, value string }{{"", "$xs"}, {"WITH $xs AS xs ", "xs"}, {"WITH {items:[$xs]} AS box ", "head(box.items)"}} {
					require.Equal(t, want, typedNumericRead(t, exec, form.prefix+query(form.value), map[string]interface{}{"xs": input.Value}), input.Name)
				}
			}
			for _, input := range listinputs.NumericInputs() {
				kind := reflect.TypeOf(input.Scalar).Kind()
				if kind == reflect.String || kind == reflect.Float32 || kind == reflect.Float64 {
					continue
				}
				want := fmt.Sprint(input.Scalar)
				suffix := ",{profile:'" + profile + "'}) AS v"
				require.Equal(t, want, typedNumericRead(t, exec, "RETURN nornicdb.json.stringify(head($xs)"+suffix, map[string]interface{}{"xs": input.List}), input.Name)
				require.Equal(t, "["+want+"]", typedNumericRead(t, exec, "RETURN nornicdb.json.stringify($xs"+suffix, map[string]interface{}{"xs": []interface{}{input.Scalar}}), input.Name)
			}
		}
	}
}
