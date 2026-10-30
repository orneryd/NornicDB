// Package apocintegers supplies the shared NOR-184 test corpus and independent
// math/big oracle. It is imported by regression tests, not production code.
// Author: Codex NOR-184 implementation lane, 2026-10-29.
package apocintegers

import (
	"math"
	"math/big"
)

// Values is tested as a Cartesian square, spanning signed values, zero,
// MinInt64 magnitudes, result bounds and intermediate multiplication overflow.
var Values = []int64{
	math.MinInt64, math.MinInt64 + 1, -math.MaxInt64, -(1 << 62),
	-6_000_000_000, -4_000_000_000, -18, -12, -1, 0, 1, 2, 12, 18,
	4_000_000_000, 6_000_000_000, 1 << 62, math.MaxInt64 - 1, math.MaxInt64,
}

// Expected uses the standard arbitrary-precision mathematical definition,
// rather than repeating the production magnitude/result-check algorithm.
func Expected(operation string, a, b int64) (int64, bool) {
	x, y := big.NewInt(a), big.NewInt(b)
	result := new(big.Int).GCD(nil, nil, x, y)
	if operation == "lcm" {
		if a == 0 || b == 0 {
			return 0, true
		}
		result.Quo(new(big.Int).Abs(x), result)
		result.Mul(result, new(big.Int).Abs(y))
	}
	if !result.IsInt64() {
		return 0, false
	}
	return result.Int64(), true
}

// Input specifies the existing checked integer binding contract independently
// of the arithmetic implementation.
type Input struct {
	Name    string
	Value   interface{}
	Integer int64
	Valid   bool
}

var Inputs = []Input{
	{"null", nil, 0, false}, {"fraction", 6.5, 0, false}, {"negative_fraction", -6.5, 0, false},
	{"nan", math.NaN(), 0, false}, {"positive_infinity", math.Inf(1), 0, false}, {"negative_infinity", math.Inf(-1), 0, false},
	{"float_above_int64", math.Ldexp(1, 63), 0, false}, {"unsigned_above_int64", uint64(1) << 63, 0, false},
	{"max_unsigned", uint64(math.MaxUint64), 0, false}, {"numeric_text", "12", 0, false}, {"boolean", true, 0, false},
	{"list", []interface{}{int64(12)}, 0, false}, {"map", map[string]interface{}{"n": int64(12)}, 0, false},
	{"signed", int64(-12), -12, true}, {"narrow_signed", int8(-12), -12, true}, {"unsigned", uint64(12), 12, true},
	{"max_unsigned_in_range", uint64(math.MaxInt64), math.MaxInt64, true}, {"whole_float", float64(6), 6, true},
	{"whole_float32", float32(-12), -12, true}, {"minimum_whole_float", -math.Ldexp(1, 63), math.MinInt64, true},
	{"negative_zero", math.Copysign(0, -1), 0, true},
}
