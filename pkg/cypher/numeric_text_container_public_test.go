// Author: L6 / #1665 lane (Codex gpt-6.1-sol). Numeric containers traverse real public output and consumer paths.
package cypher

import (
	"context"
	"math"
	"strings"
	"testing"

	apocexport "github.com/orneryd/nornicdb/apoc/export"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/numericoracle"
	"github.com/stretchr/testify/require"
)

func TestNumericTextPublicContainerConversion(t *testing.T) {
	exec := numericTextExecutor(t, "apoc.convert.toString", "apoc.convert.toStringList")
	for _, input := range []struct {
		name  string
		value interface{}
		text  string
	}{
		{"boxed", []interface{}{float64(2), int64(2)}, "[2.0 2]"},
		{"typed_float", []float64{2, math.Copysign(0, -1)}, "[2.0 -0.0]"},
		{"nested", []interface{}{[]interface{}{float64(2), int64(9007199254740993)}}, "[[2.0 9007199254740993]]"},
		{"map", map[string]interface{}{"f": float64(2), "i": int64(2)}, "map[f:2.0 i:2]"},
	} {
		t.Run(input.name, func(t *testing.T) {
			got := numericTextQuery(t, exec, "RETURN apoc.convert.toString($v) AS text", map[string]interface{}{"v": input.value})
			require.Equal(t, input.text, got)
			got = numericTextQuery(t, exec, "RETURN apoc.convert.toStringList([$v]) AS text", map[string]interface{}{"v": input.value})
			numericoracle.Equal(t, []interface{}{input.text}, got)
		})
	}
}

func TestNumericTextPublicCypherContainerExport(t *testing.T) {
	exporter := numericTextExecutor(t, "apoc.export.cypher")
	for _, input := range []struct {
		name        string
		value, want interface{}
		storable    bool
	}{
		{"expression_mixed_not_property", []interface{}{float64(2), int64(9007199254740993), "2.0"}, []interface{}{float64(2), int64(9007199254740993), "2.0"}, false},
		{"typed_integer", []int64{9007199254740993, -9007199254740993}, []interface{}{int64(9007199254740993), int64(-9007199254740993)}, true},
		{"typed_float", []float64{2, math.Copysign(0, -1)}, []interface{}{float64(2), math.Copysign(0, -1)}, true},
		{"typed_float32_whole", []float32{2, 3}, []interface{}{float64(2), float64(3)}, true},
		{"expression_nested_list_not_property", []interface{}{[]interface{}{float64(2), int64(2)}}, []interface{}{[]interface{}{float64(2), int64(2)}}, false},
		{"expression_map_not_property", map[string]interface{}{"f": float64(2), "i": int64(2)}, map[string]interface{}{"f": float64(2), "i": int64(2)}, false},
	} {
		t.Run(input.name, func(t *testing.T) {
			nodes := []*apocexport.Node{{ID: 1, Labels: []string{"NumericContainer"}, Properties: map[string]interface{}{"v": input.value}}}
			text := numericTextQuery(t, exporter, "RETURN apoc.export.cypher($nodes,[]) AS text", map[string]interface{}{"nodes": nodes}).(string)
			consumer := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "numeric_container"))
			_, err := consumer.Execute(context.Background(), text, nil)
			if !input.storable {
				// These are expression values, not supported property carriers.
				// Preserve CREATE's existing admission rule and independently
				// consume the actual exported literal through public RETURN.
				require.ErrorContains(t, err, "Neo.ClientError.Statement.TypeError")
				const prefix = "CREATE (n1:NumericContainer {v: "
				const suffix = "});\n"
				require.True(t, strings.HasPrefix(text, prefix), "actual exported statement: %s", text)
				require.True(t, strings.HasSuffix(text, suffix), "actual exported statement: %s", text)
				literal := strings.TrimSuffix(strings.TrimPrefix(text, prefix), suffix)
				got := numericTextQuery(t, consumer, "RETURN "+literal+" AS value", nil)
				numericoracle.Equal(t, input.want, got)
				return
			}
			require.NoError(t, err, "consume the actual exported source: %s", text)
			got := numericTextQuery(t, consumer, "MATCH (n:NumericContainer) RETURN n.v AS value", nil)
			numericoracle.Equal(t, input.want, got)
		})
	}
}
