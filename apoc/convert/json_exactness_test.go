package convert

import (
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func TestJSONIngestionLibraryConvert(t *testing.T) {
	m, err := FromJsonMap(`{"v":9007199254740993}`)
	require.NoError(t, err)
	require.Equal(t, int64(9007199254740993), m["v"])
	list, err := FromJsonList(`[9007199254740993,2.0]`)
	require.NoError(t, err)
	require.Equal(t, []interface{}{int64(9007199254740993), float64(2)}, list)
	m, err = ToMap(`{"v":9007199254740993}`)
	require.NoError(t, err)
	require.Equal(t, int64(9007199254740993), m["v"])
	// The external-number policy's rejection reaches every JSON consumer.
	for _, call := range []func() error{
		func() error { _, err := FromJsonMap(`{"v":9223372036854775808}`); return err },
		func() error { _, err := FromJsonList(`[9223372036854775808]`); return err },
		func() error { _, err := ToMap(`{"v":9223372036854775808}`); return err },
		func() error { _, err := FromJsonNode(`{"v":9223372036854775808}`); return err },
		func() error { _, err := GetJsonProperty(`{"v":9223372036854775808}`, "v"); return err },
		func() error { _, err := GetJsonPropertyMap(`{"v":9223372036854775808}`, "v"); return err },
		func() error { _, err := SetJsonProperty(`{"v":9223372036854775808}`, "w", 1); return err },
	} {
		require.ErrorContains(t, call(), "Unsupported type of Number 9223372036854775808")
	}
	infinite, err := FromJsonList(`[1e400]`)
	require.NoError(t, err)
	require.Equal(t, []interface{}{math.Inf(1)}, infinite)
}
