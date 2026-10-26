package antlr

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

// Author: #1663 lane (relocation). Existing release controls retain their Git
// provenance; authorship is not inferred from the merged owner's allocation.
func TestKnowledgeQuotedStringValues(t *testing.T) {
	tests := []struct {
		name  string
		input string
		value string
		end   int
	}{
		{
			name:  "TestKpScanQuotedString_SingleQuote",
			input: "'hello world'",
			value: "hello world",
			end:   13,
		},
		{
			name:  "TestKpScanQuotedString_DoubleQuote",
			input: `"hello"`,
			value: "hello",
			end:   7,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			val, j := kpScanQuotedString(test.input, 0)
			assert.Equal(t, test.value, val)
			assert.Equal(t, test.end, j)
		})
	}
}

func TestKpScanDecimal_Negative(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    string
		floating bool
		want     any
		end      int
	}{
		{name: "floating", input: "-3.14", floating: true, want: float64(-3.14), end: 5},
		{name: "integer", input: "-99", want: int64(-99), end: 3},
		{name: "TestKpScanNumber_Integer", input: "42", floating: true, want: float64(42.0), end: 2},
		{name: "TestKpScanInt_Valid", input: "12345", floating: false, want: int64(12345), end: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var value any
			var end int
			var ok bool
			if tc.floating {
				value, end, ok = kpScanDecimal[float64](tc.input, 0)
			} else {
				value, end, ok = kpScanDecimal[int64](tc.input, 0)
			}
			require.True(t, ok)
			assert.Equal(t, tc.want, value)
			assert.Equal(t, tc.end, end)
		})
	}
}

func TestKpScanNumber_InvalidInput(t *testing.T) {
	_, _, ok := kpScanDecimal[float64]("abc", 0)
	assert.False(t, ok)
}

func TestKnowledgeBooleanValues(t *testing.T) {
	for _, test := range []struct {
		name  string
		input string
		value bool
		end   int
	}{
		{"TestKpScanBool_True", "TRUE rest", true, 4},
		{"TestKpScanBool_False", "FALSE rest", false, 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			value, end, ok := kpScanBool(test.input, 0)
			require.True(t, ok)
			assert.Equal(t, test.value, value)
			assert.Equal(t, test.end, end)
		})
	}
}

func TestKpScanBool_Invalid(t *testing.T) {
	_, _, ok := kpScanBool("MAYBE", 0)
	assert.False(t, ok)
}

func TestKpScanQuotedString_Unterminated(t *testing.T) {
	_, j := kpScanQuotedString("'unterminated", 0)
	assert.Equal(t, -1, j)
}

func TestKpDecimalPreservesIntegerPrecisionAndPolicy(t *testing.T) {
	for _, raw := range []string{"9223372036854775807", "-9223372036854775808", "9007199254740993"} {
		expected, err := strconv.ParseInt(raw, 10, 64)
		require.NoError(t, err)
		value, end, ok := kpScanDecimal[int64](raw, 0)
		require.True(t, ok, raw)
		require.Equal(t, expected, value)
		require.Equal(t, len(raw), end)
	}
	for _, raw := range []string{"9223372036854775808", "-9223372036854775809", "1.5", "1e2", "1_000", "0x10", "42n"} {
		_, end, ok := kpScanDecimal[int64](raw, 0)
		require.False(t, ok, raw)
		require.Zero(t, end, raw)
	}
	value, end, ok := kpScanDecimal[float64]("1e2", 0)
	require.True(t, ok)
	require.Equal(t, 100.0, value)
	require.Equal(t, 3, end)
}
