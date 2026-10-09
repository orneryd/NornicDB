package libm

import "math"

// asUint64 returns the IEEE 754 double-precision bit representation of x.
// It mirrors musl's asuint64 macro.
func asUint64(x float64) uint64 { return math.Float64bits(x) }

// asFloat64 returns the double whose IEEE 754 bit representation is i.
// It mirrors musl's asdouble macro.
func asFloat64(i uint64) float64 { return math.Float64frombits(i) }

// evalAsDouble is musl's eval_as_double: on Go there is no excess-precision
// evaluation, so it is the identity.
func evalAsDouble(x float64) float64 { return x }

// top12 returns the top 12 bits of x (sign and exponent), as musl's top12.
func top12(x float64) uint64 { return asUint64(x) >> 52 }

// uflow returns the correctly signed underflow result (a zero).
func uflow(negative bool) float64 {
	if negative {
		return math.Copysign(0, -1)
	}
	return 0
}

// oflow returns the correctly signed overflow result (an infinity).
func oflow(negative bool) float64 {
	if negative {
		return math.Inf(-1)
	}
	return math.Inf(1)
}

// inf is IEEE 754 positive infinity; nan is a quiet NaN. These shadow the
// math package names so ported bodies read like the musl source.
var (
	inf = math.Inf(1)
	nan = math.NaN()
)

// top12u32 returns the top 12 bits of x as a 32-bit word, matching musl's
// uint32_t top12 used by the exp/pow family.
func top12u32(x float64) uint32 { return uint32(asUint64(x) >> 52) }

// top16u32 returns the top 16 bits of x as a 32-bit word, matching musl's
// uint32_t top16 used by the log family.
func top16u32(x float64) uint32 { return uint32(asUint64(x) >> 48) }
