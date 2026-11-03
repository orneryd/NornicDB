package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNumericWireAPOCFloatRoundTrip(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	result, err := executor.Execute(context.Background(), `RETURN apoc.convert.fromJsonMap(apoc.convert.toJson({v:2.0,nested:[3.0],big:9007199254740993}))`, nil)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"v": float64(2), "nested": []interface{}{float64(3)}, "big": int64(9007199254740993)}, result.Rows[0][0])
}
