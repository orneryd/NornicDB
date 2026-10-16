package fn

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPropertiesKeepsOrdinaryNestedPropertiesMap(t *testing.T) {
	object := map[string]interface{}{"properties": map[string]interface{}{"x": int64(1)}, "other": int64(2)}
	got, found, err := EvaluateFunction("properties", []string{"m"}, Context{Eval: func(string) (interface{}, error) { return object, nil }})
	require.True(t, found)
	require.NoError(t, err)
	require.Equal(t, object, got)
}
