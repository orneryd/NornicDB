// Package convert provides type conversion utilities for NornicDB.
//
// This package consolidates type conversion functions used throughout the codebase,
// ensuring consistent behavior for numeric conversions, slice transformations,
// and string parsing.
//
// Key Functions:
//   - ToFloat64: Convert various types to float64
//   - ToInt64: Convert various types to int64
//   - ToFloat64Slice: Convert slices to []float64
//   - ToFloat32Slice: Convert slices to []float32
//
// All conversion functions return a success boolean to allow callers to handle
// conversion failures gracefully.
//
// Example:
//
//	// Convert any numeric type to float64
//	if f, ok := convert.ToFloat64(someValue); ok {
//		// Use f
//	}
//
//	// Convert slice to []float32 for vector operations
//	floats := convert.ToFloat32Slice(data)
//	if floats != nil {
//		// Use floats
//	}
//
// ELI12:
//
// This package is like a universal translator for numbers. You give it any kind
// of number (whole number, decimal, even text that looks like a number), and it
// converts it to the type you need. If it can't convert something (like "hello"),
// it tells you by returning false or nil.
package convert

import (
	"math"
	"math/big"
	"strconv"
	"strings"
)

// ToFloat64 converts various numeric types to float64.
// Returns (value, true) on success, (0, false) on failure.
//
// This is the STANDARD conversion function for all numeric types.
// Use this instead of custom type switches throughout the codebase.
//
// Supported types:
//   - float64 (returned as-is)
//   - float32 (converted)
//   - int, int32, int64 (lossless for values within float64 range)
//   - string (parsed as decimal, supports scientific notation)
//
// String parsing supports:
//   - Decimal: "3.14", "-2.5"
//   - Scientific notation: "1.23e-4", "6.02e23"
//   - Special values: "NaN", "Inf", "-Inf"
//
// Example 1 - Numeric types:
//
//	f, ok := ToFloat64(42)        // Returns (42.0, true)
//	f, ok := ToFloat64(int64(99)) // Returns (99.0, true)
//	f, ok := ToFloat64(3.14)      // Returns (3.14, true)
//
// Example 2 - String parsing:
//
//	f, ok := ToFloat64("3.14")    // Returns (3.14, true)
//	f, ok := ToFloat64("1.5e-3")  // Returns (0.0015, true)
//	f, ok := ToFloat64("invalid") // Returns (0, false)
//
// Example 3 - Error handling:
//
//	value := "user_input"
//	if f, ok := ToFloat64(value); ok {
//		result := f * 2.0
//	} else {
//		return fmt.Errorf("invalid number: %s", value)
//	}
//
// ELI12:
//
// This function is like a universal translator for numbers. You give it
// any kind of number (whole number, decimal, even text that looks like
// a number), and it converts it to a decimal number (float64).
// If it can't convert it (like trying to convert "hello" to a number),
// it tells you by returning false.
func ToFloat64(v interface{}) (float64, bool) {
	if number, ok := NumericFloat64(v); ok {
		return number, true
	}
	if text, ok := v.(string); ok {
		if f, err := strconv.ParseFloat(text, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

// ToInt64 converts various numeric types to int64.
// Returns (value, true) on success, (0, false) on failure.
//
// Supported types:
//   - int64 (returned as-is)
//   - int, int32, int16, int8 (converted)
//   - uint, uint32, uint16, uint8, uint64 (converted if within int64 range)
//   - float64, float32 (truncated toward zero)
//   - string (parsed as integer)
//
// Example:
//
//	i, ok := ToInt64(42)        // Returns (42, true)
//	i, ok := ToInt64(3.7)       // Returns (3, true) - truncated
//	i, ok := ToInt64("123")     // Returns (123, true)
//	i, ok := ToInt64("invalid") // Returns (0, false)
//
// ELI12:
//
// This converts numbers to whole numbers (integers). If you give it
// a decimal like 3.7, it chops off the .7 part and gives you 3.
func ToInt64(v interface{}) (int64, bool) {
	return Int64(v, IntegerFloat|IntegerStrings|IntegerDecimalStrings)
}

// IntegerPolicy selects the caller's existing coercion contract. Range checks
// and numeric type recognition remain here, including for plugin target types.
type IntegerPolicy uint16

const IntegerOnly IntegerPolicy = 0

const (
	IntegerFloat IntegerPolicy = 1 << iota
	IntegerWholeFloat
	IntegerSaturate
	IntegerStrings
	IntegerDecimalStrings
	IntegerBool
	IntegerTrimStrings
	IntegerClampBounds
)

// CypherIntegerConversion follows numeric toInteger/toIntegerList: a floating
// value saturates (NaN is zero); a string must fit after exact decimal truncation.
const CypherIntegerConversion = IntegerFloat | IntegerSaturate | IntegerStrings | IntegerDecimalStrings | IntegerBool | IntegerTrimStrings

// Int64 is the checked narrowing owner. Unsigned overflow always fails. Floating
// overflow either fails or saturates according to policy, never a Go cast's
// platform-dependent result. Strings never use float64 as an integer intermediate.
func Int64(value interface{}, policy IntegerPolicy) (int64, bool) {
	if text, ok := value.(string); ok {
		if policy&IntegerStrings == 0 {
			return 0, false
		}
		if policy&IntegerTrimStrings != 0 {
			text = strings.TrimSpace(text)
		}
		if integer, err := strconv.ParseInt(text, 10, 64); err == nil {
			return integer, true
		}
		if policy&IntegerDecimalStrings != 0 {
			if integer, ok := decimalInteger(text); ok && integer.IsInt64() {
				return integer.Int64(), true
			}
		}
		return 0, false
	}
	if boolean, ok := value.(bool); ok {
		if policy&IntegerBool == 0 {
			return 0, false
		}
		if boolean {
			return 1, true
		}
		return 0, true
	}
	number, ok := ReadNumber(value)
	if !ok {
		return 0, false
	}
	return number.Int64(policy)
}

// Int64 narrows an already-read number through the same checked boundary.
func (number Number) Int64(policy IntegerPolicy) (int64, bool) {
	switch number.kind {
	case SignedNumber:
		return int64(number.bits), true
	case UnsignedNumber:
		return unsignedInt64(number.bits, policy&IntegerClampBounds != 0)
	case FloatNumber:
		if policy&IntegerFloat != 0 {
			return floatInt64(number.Float64(), policy)
		}
	}
	return 0, false
}

func floatInt64(value float64, policy IntegerPolicy) (int64, bool) {
	saturate := policy&IntegerSaturate != 0
	if math.IsNaN(value) {
		return 0, saturate
	}
	if policy&IntegerWholeFloat != 0 && (math.IsInf(value, 0) || math.Trunc(value) != value) {
		return 0, false
	}
	clamp := saturate || policy&IntegerClampBounds != 0 && !math.IsInf(value, 0)
	// float64(MaxInt64) rounds to 2^63, so that upper endpoint is exclusive.
	if value >= 0x1p63 {
		if clamp {
			return math.MaxInt64, true
		}
		return 0, false
	}
	if value < -0x1p63 {
		if clamp {
			return math.MinInt64, true
		}
		return 0, false
	}
	return int64(value), true
}

// decimalInteger validates the existing numeric-string grammar, then truncates
// its exact rational value. This preserves near-boundary decimal precision and
// excludes nonfinite, fraction-notation and range-overflow strings.
func decimalInteger(text string) (*big.Int, bool) {
	if _, err := strconv.ParseFloat(text, 64); err != nil {
		return nil, false
	}
	rational, ok := new(big.Rat).SetString(text)
	if !ok {
		return nil, false
	}
	return new(big.Int).Quo(rational.Num(), rational.Denom()), true
}

// ToInt applies the same conversion and checks the native int width afterwards.
func ToInt(value interface{}, policy IntegerPolicy) (int, bool) {
	integer, ok := Int64Bits(value, strconv.IntSize, policy)
	return int(integer), ok
}

func Int64Bits(value interface{}, bits int, policy IntegerPolicy) (int64, bool) {
	integer, ok := Int64(value, policy)
	if !ok {
		return 0, false
	}
	return signedBits(integer, bits, policy&IntegerClampBounds != 0)
}

func (number Number) Int64Bits(bits int, policy IntegerPolicy) (int64, bool) {
	integer, ok := number.Int64(policy)
	if !ok {
		return 0, false
	}
	return signedBits(integer, bits, policy&IntegerClampBounds != 0)
}

func signedBits(integer int64, bits int, clamp bool) (int64, bool) {
	if bits < 1 || bits > 64 {
		return 0, false
	}
	maximum := int64((uint64(1) << (bits - 1)) - 1)
	minimum := -maximum - 1
	if integer < minimum {
		if clamp {
			return minimum, true
		}
		return 0, false
	}
	if integer > maximum {
		if clamp {
			return maximum, true
		}
		return 0, false
	}
	return integer, true
}

// ToUint64 is the unsigned target of the same checked numeric conversion. It is
// used by typed plugin signatures; fractional/nonfinite values remain invalid.
func ToUint64(value interface{}, policy IntegerPolicy) (uint64, bool) {
	if text, ok := value.(string); ok {
		if policy&IntegerStrings == 0 {
			return 0, false
		}
		if policy&IntegerTrimStrings != 0 {
			text = strings.TrimSpace(text)
		}
		if integer, err := strconv.ParseUint(text, 10, 64); err == nil {
			return integer, true
		}
		if policy&IntegerDecimalStrings != 0 {
			if integer, ok := decimalInteger(text); ok && integer.IsUint64() {
				return integer.Uint64(), true
			}
		}
		return 0, false
	}
	number, ok := ReadNumber(value)
	if !ok {
		return 0, false
	}
	return number.Uint64(policy)
}

// Uint64 narrows an already-read number to an unsigned plugin target.
func (number Number) Uint64(policy IntegerPolicy) (uint64, bool) {
	switch number.kind {
	case UnsignedNumber:
		return number.bits, true
	case SignedNumber:
		integer, _ := number.Signed()
		if integer >= 0 {
			return number.bits, true
		}
		return 0, false
	case FloatNumber:
		floating := number.Float64()
		if policy&IntegerFloat == 0 || math.IsNaN(floating) || math.IsInf(floating, 0) || policy&IntegerWholeFloat != 0 && math.Trunc(floating) != floating {
			return 0, false
		}
		floating = math.Trunc(floating)
		if floating < 0 || floating >= 0x1p64 {
			return 0, false
		}
		return uint64(floating), true
	}
	return 0, false
}

func (number Number) Uint64Bits(bits int, policy IntegerPolicy) (uint64, bool) {
	integer, ok := number.Uint64(policy)
	if !ok || bits < 1 || bits > 64 {
		return 0, false
	}
	if bits < 64 && integer > (uint64(1)<<bits)-1 {
		return 0, false
	}
	return integer, true
}

// unsignedInt64 is the single checked narrowing path for unsigned types that
// can exceed Cypher's signed integer range. Narrower unsigned types always fit.
func unsignedInt64(value uint64, clamp bool) (int64, bool) {
	if value > math.MaxInt64 {
		if clamp {
			return math.MaxInt64, true
		}
		return 0, false
	}
	return int64(value), true
}
