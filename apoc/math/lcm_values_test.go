// Author: Codex implementation lane for NOR-184, 2026-10-26.
package math_test

import (
	"math"
	"testing"

	apocmath "github.com/orneryd/nornicdb/apoc/math"
	"github.com/orneryd/nornicdb/apoc/number"
	"github.com/stretchr/testify/assert"
)

func TestLcmValuesNOR184(t *testing.T) {
	for _, tc := range []struct {
		name       string
		a, b, want int64
	}{
		{"zero_pair", 0, 0, 0},
		{"left_zero", 0, 5, 0},
		{"right_zero", 5, 0, 0},
		{"negative_with_zero", -5, 0, 0},
		{"minimum_with_zero", math.MinInt64, 0, 0},
		{"common_factor", 12, 18, 36},
		{"coprime", 7, 11, 77},
		{"left_negative", -12, 18, 36},
		{"right_negative", 12, -18, 36},
		{"both_negative", -12, -18, 36},
		{"divide_before_multiply", 4_000_000_000, 6_000_000_000, 12_000_000_000},
		{"maximum_identity", math.MaxInt64, math.MaxInt64, math.MaxInt64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for name, fn := range map[string]interface{}{"math": apocmath.Lcm, "number": number.LCM} {
				t.Run(name, func(t *testing.T) {
					got, err, panicked := integerCallNOR184(fn, tc.a, tc.b)
					assert.Nil(t, panicked)
					assert.NoError(t, err)
					assert.Equal(t, tc.want, got)
				})
			}
		})
	}
}

// Retain the original positive smoke cases alongside the complete signed and
// overflow class matrix in TestIntegerContractNOR184.
func TestGcdAliasValuesNOR184(t *testing.T) {
	for _, tc := range []struct{ a, b, want int64 }{
		{0, 0, 0}, {0, 5, 5}, {5, 0, 5}, {48, 18, 6}, {18, 48, 6},
		{math.MaxInt64, math.MaxInt64, math.MaxInt64},
	} {
		for name, fn := range map[string]interface{}{"math": apocmath.Gcd, "number": number.GCD} {
			t.Run(name, func(t *testing.T) {
				got, err, panicked := integerCallNOR184(fn, tc.a, tc.b)
				assert.Nil(t, panicked)
				assert.NoError(t, err)
				assert.Equal(t, tc.want, got)
			})
		}
	}
}
