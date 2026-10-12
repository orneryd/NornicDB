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

func TestPromoteConstantNumbers(t *testing.T) {
	const big, float = "9007199254740993", "9007199254740992.0"
	for _, tc := range []struct {
		name                  string
		operator, left, right string
		leftValue, rightValue interface{}
		wantLeft, wantRight   interface{}
	}{
		{"integer = float literal", "=", big, float, int64(9007199254740993), 9007199254740992.0, 9007199254740993.0, 9007199254740992.0},
		{"float > integer literal", ">", float, big, 9007199254740992.0, int64(9007199254740993), 9007199254740992.0, 9007199254740993.0},
		{"<> compares exactly", "<>", big, float, int64(9007199254740993), 9007199254740992.0, int64(9007199254740993), 9007199254740992.0},
		{"a variable compares exactly", "=", "x", float, int64(9007199254740993), 9007199254740992.0, int64(9007199254740993), 9007199254740992.0},
		{"two integers", "=", "1", "1", int64(1), int64(1), int64(1), int64(1)},
		{"unsigned against a float", "<", "1", "2.0", uint64(1), 2.0, uint64(1), 2.0},
		{"float against unsigned", "<", "2.0", "1", 2.0, uint64(1), 2.0, uint64(1)},
	} {
		left, right := promoteConstantNumbers(tc.operator, tc.left, tc.right, tc.leftValue, tc.rightValue, false)
		require.Equal(t, tc.wantLeft, left, tc.name)
		require.Equal(t, tc.wantRight, right, tc.name)
	}
	// Cypher 25 folds <> too (Neo4j 2026.09).
	for _, operator := range []string{"<>", "!="} {
		left, right := promoteConstantNumbers(operator, big, float, int64(9007199254740993), 9007199254740992.0, true)
		require.Equal(t, 9007199254740993.0, left, operator)
		require.Equal(t, 9007199254740992.0, right, operator)
	}
}
