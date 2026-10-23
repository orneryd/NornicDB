package convert

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestJSONIngestionLibraryConvert(t *testing.T) {
	require.Equal(t, int64(9007199254740993), FromJsonMap(`{"v":9007199254740993}`)["v"])
	require.Equal(t, []interface{}{int64(9007199254740993), float64(2)}, FromJsonList(`[9007199254740993,2.0]`))
	require.Equal(t, int64(9007199254740993), ToMap(`{"v":9007199254740993}`)["v"])
}
