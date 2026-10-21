package config

import (
	"errors"
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

func TestRuntimeLimitAliasesPreserveAutomaticMode(t *testing.T) {
	for _, pair := range [][2]string{{"auto", "0"}, {"0", "auto"}} {
		_, err := ResolveRuntimeLimitAliases(pair[0], pair[1])
		if pair[1] == "auto" {
			require.Error(t, err) // Canonical settings only accept byte integers.
		} else {
			require.True(t, errors.Is(err, ErrRuntimeLimitAliasConflict), "%q / %q: %v", pair[0], pair[1], err)
		}
	}
	choice, err := ResolveRuntimeLimitAliases("AUTO", "")
	require.NoError(t, err)
	require.True(t, choice.Auto)
	require.Equal(t, "AUTO", choice.Raw)
	choice, err = ResolveRuntimeLimitAliases("1024", "1073741824")
	require.NoError(t, err)
	require.False(t, choice.Auto)
	require.Equal(t, int64(1024<<20), choice.Bytes)
	for _, raw := range []string{"auto", "2g", "-1", "1.5"} {
		_, err := ResolveRuntimeLimitAliases("", raw)
		require.Error(t, err, raw)
	}
}

func TestRuntimeLimitByteIntegerContract(t *testing.T) {
	choice, err := ResolveRuntimeLimitAliases("", "1024")
	require.NoError(t, err)
	require.Equal(t, int64(1024), choice.Bytes)
	_, err = ResolveRuntimeLimitAliases("1kb", "")
	require.Error(t, err)
	_, err = ResolveRuntimeLimitAliases("", "1kb")
	require.Error(t, err)
}
