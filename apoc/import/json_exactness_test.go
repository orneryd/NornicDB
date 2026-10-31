package apocimport

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestJSONIngestionLibraryImport(t *testing.T) {
	value, err := ParseJsonLine(`{"properties":{"v":9007199254740993,"d":2.0}}`)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"properties": map[string]interface{}{"v": int64(9007199254740993), "d": float64(2)}}, value)
	result, err := JsonData(`{"nodes":[{"properties":{"v":9007199254740993,"d":2.0}}]}`)
	require.NoError(t, err)
	require.Equal(t, 1, result.Nodes)
	require.Equal(t, 2, result.Properties)
}
