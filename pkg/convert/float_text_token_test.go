package convert

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSmallWholeFloatTextMatchesWriter holds every static whole-FLOAT spelling
// to the writer's own output at both precisions. The static tokens are an
// allocation shortcut, never a second spelling rule, so the test compares
// against appendFloatText instead of restating the table.
func TestSmallWholeFloatTextMatchesWriter(t *testing.T) {
	// The lookup slices a fixed-width window out of one string, so a token of
	// another width would silently shift every later spelling.
	require.Equal(t, 360, len(wholeTensFloatText), "the ten-through-ninety-nine table is not 90 four-byte tokens")
	for _, bits := range []int{32, 64} {
		for value := 0; value < 100; value++ {
			whole := float64(value)
			token, wholeToken := smallWholeFloatText(whole)
			require.True(t, wholeToken, "bits=%d value=%d: whole FLOAT was declined", bits, value)
			require.Equal(t, appendFloatText(nil, whole, bits), []byte(token), "bits=%d value=%d", bits, value)
			require.Equal(t, token, FormatFloat(whole, bits), "bits=%d value=%d", bits, value)

			narrow := float64(float32(whole))
			narrowToken, narrowWhole := smallWholeFloatText(narrow)
			require.True(t, narrowWhole, "bits=32 value=%d: whole float32 was declined", value)
			require.Equal(t, appendFloatText(nil, narrow, 32), []byte(narrowToken), "bits=32 value=%d", value)
			require.Equal(t, narrowToken, FormatFloat(narrow, 32), "bits=32 value=%d", value)
		}
	}
	negativeZero := math.Copysign(0, -1)
	for _, bits := range []int{32, 64} {
		token, whole := smallWholeFloatText(negativeZero)
		require.True(t, whole)
		require.Equal(t, string(appendFloatText(nil, negativeZero, bits)), token, "bits=%d", bits)
		require.Equal(t, token, FormatFloat(negativeZero, bits), "bits=%d", bits)
		require.Equal(t, "-0.0", token)
		require.Equal(t, "-0.0", FormatFloat(float64(float32(negativeZero)), 32))
	}
	// Values outside the static spellings keep the writer's own text, and the
	// token lookup declines them instead of rounding them into a token.
	for _, value := range []float64{
		-100, -99, -10, -9, -1, 100, 1000, 1e6, 9.5, 99.0000001, 99.5, 100.5,
		1e-3, -1e-3, 1e7, 0.001, -0.001, math.SmallestNonzeroFloat64,
		math.SmallestNonzeroFloat64 * -1, math.MaxFloat64, math.Inf(1), math.NaN(),
	} {
		if token, whole := smallWholeFloatText(value); whole {
			t.Fatalf("value=%v was reported as the static token %q", value, token)
		}
		if math.IsNaN(value) || math.IsInf(value, 0) {
			continue
		}
		require.Equal(t, string(appendFloatText(nil, value, 64)), FormatFloat(value, 64), "value=%v", value)
	}
}

// TestSmallWholeFloatTextAllocationFree keeps the corrected '.0' token from
// costing a runtime string copy: 0 through 99 are immutable spellings at both
// precisions. The same check keeps the shortcut honest - the spelled values are
// the ones that became allocation-free, so a table that stopped short of 99
// would fail here rather than pass by falling back to the writer.
func TestSmallWholeFloatTextAllocationFree(t *testing.T) {
	for _, bits := range []int{32, 64} {
		for value := 0; value < 100; value++ {
			whole := float64(value)
			want, ok := smallWholeFloatText(whole)
			require.True(t, ok)
			var formatted string
			allocs := testing.AllocsPerRun(20, func() {
				formatted = FormatFloat(whole, bits)
			})
			require.Equal(t, want, formatted, "bits=%d value=%d", bits, value)
			require.Zero(t, allocs, "bits=%d value=%d: %q was copied instead of spelled", bits, value, want)
		}
	}
	// The shortcut is bounded: a whole FLOAT outside it still gets the writer's
	// own text, and one allocation of that text is the required result rather
	// than a shortcut miss.
	var outside string
	allocs := testing.AllocsPerRun(20, func() {
		outside = FormatFloat(100, 64)
	})
	require.Equal(t, string(appendFloatText(nil, 100, 64)), outside)
	require.Equal(t, 1.0, allocs)
}
