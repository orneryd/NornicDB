package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestParameterContainerCancellationBoundaries(t *testing.T) {
	phases := make([]int, pipelineCancellationInterval)
	for i := range phases {
		phases[i] = i + 1
	}
	fields := map[string]interface{}{}
	list := make([]interface{}, 129)
	array := [129]map[string]int{}
	for i := range list {
		fields[fmt.Sprintf("field%d", i)] = []int{1, 2}
		list[i] = map[string]int{"value": i}
		array[i] = map[string]int{"value": i}
	}
	values := []interface{}{
		fields, list, array, map[string]interface{}{"changed": map[string]int{"value": 1}},
		map[string][]map[string]int{"values": array[:]},
		[]interface{}{[]int{1, 2}, []string{"x", "y"}, []bool{true, false}},
		map[interface{}]interface{}{"key": strings.Repeat("'\\", 4097), 1: "ignored"},
		[]map[interface{}]interface{}{{"key": 1}, {"key": 2}},
		struct{ Text string }{strings.Repeat("text", 1025)},
	}
	for index, value := range values {
		for _, kind := range []string{"normalize", "row"} {
			t.Run(fmt.Sprintf("%d/%s", index, kind), func(t *testing.T) {
				runPreparationBudgetSweep(t, func(_ context.Context, probe *pipelineCancellation) {
					switch kind {
					case "normalize":
						normalizeQueryParametersWithCancellation(map[string]interface{}{"value": value}, probe)
					case "row":
						parameterRowValueWithCancellation(value, probe)
					}
				}, phases...)
			})
		}
	}
}
