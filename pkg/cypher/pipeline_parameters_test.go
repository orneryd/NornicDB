package cypher

import (
	"context"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPipelineProjectionParametersStayTyped(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "typed_projection_params"))
	for _, query := range []string{
		"WITH $value AS value RETURN value",
		"CALL () { WITH $value AS value RETURN value } RETURN value",
		"CALL () { CALL () { WITH $value AS value RETURN value } RETURN value } RETURN value",
	} {
		result, err := exec.Execute(context.Background(), query, map[string]interface{}{"value": 1.0})
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{1.0}}, result.Rows, query)
	}
	result, err := exec.Execute(context.Background(), "WITH $payload AS payload CALL (payload) { WITH payload AS result RETURN result } RETURN result", map[string]interface{}{
		"payload": map[string]interface{}{"number": 1.0, "nested": []interface{}{nil, math.NaN()}},
	})
	require.NoError(t, err)
	value := result.Rows[0][0].(map[string]interface{})
	require.Equal(t, 1.0, value["number"])
	list := value["nested"].([]interface{})
	require.Nil(t, list[0])
	require.True(t, math.IsNaN(list[1].(float64)))
	result, err = exec.Execute(context.Background(), "WITH $number AS number CALL () { CREATE (n:TypedParameter {number: $number}) RETURN n.number AS stored } RETURN number, stored", map[string]interface{}{"number": int64(7)})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(7), int64(7)}}, result.Rows)
	_, err = exec.Execute(context.Background(), "CALL () { } RETURN 1 AS value", map[string]interface{}{"number": int64(7)})
	require.Error(t, err, "an empty parameterized CALL body still fails")
}
