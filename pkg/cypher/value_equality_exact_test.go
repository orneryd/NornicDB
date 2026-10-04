package cypher

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// compareCypherNumbersExactly orders an integer against a float by exact
// values across the integer kinds and float widths (#893).
func TestCompareCypherNumbersExactly(t *testing.T) {
	for _, tc := range []struct {
		name        string
		left, right interface{}
		comparison  int
		ok          bool
	}{
		{"integers", int64(3), int64(2), 1, true},
		{"integer above a float", int64(9007199254740993), 9007199254740992.0, 1, true},
		{"float below an integer", 9007199254740992.0, int64(9007199254740993), -1, true},
		{"equal", int64(2), 2.0, 0, true},
		{"fraction above", int64(2), 2.5, -1, true},
		{"negative fraction", int64(-2), -2.5, 1, true},
		{"float32", int64(1), float32(1.5), -1, true},
		{"float beyond the range", int64(math.MaxInt64), 0x1p63, -1, true},
		{"float below the range", int64(math.MinInt64), -0x1p64, 1, true},
		{"unsigned against a negative float", uint64(1), -1.0, 1, true},
		{"unsigned against a huge float", uint64(math.MaxUint64), 0x1p64, -1, true},
		{"unsigned equal", uint32(7), 7.0, 0, true},
		{"unsigned fraction", uint64(7), 7.25, -1, true},
		{"unsigned above", uint64(8), 7.5, 1, true},
		{"NaN", int64(1), math.NaN(), 0, false},
		{"two floats", 1.0, 2.0, 0, false},
		{"not numbers", "a", 1.0, 0, false},
	} {
		comparison, ok := compareCypherNumbersExactly(tc.left, tc.right)
		require.Equal(t, tc.ok, ok, tc.name)
		require.Equal(t, tc.comparison, comparison, tc.name)
	}
}
