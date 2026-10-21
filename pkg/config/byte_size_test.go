package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseByteSize(t *testing.T) {
	tests := map[string]int64{
		"0":    0,
		"512":  512,
		"1k":   1 << 10,
		"2M":   2 << 20,
		"3 g ": 3 << 30,
		"1t":   1 << 40,
	}
	for input, expected := range tests {
		actual, err := ParseByteSize(input)
		require.NoError(t, err, input)
		require.Equal(t, expected, actual, input)
	}
	for _, input := range []string{"", "-1", "1kb", "1.5m", "overflow999999999999999999999t"} {
		_, err := ParseByteSize(input)
		require.Error(t, err, input)
	}
}
