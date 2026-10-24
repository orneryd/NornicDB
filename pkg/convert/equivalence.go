package convert

import (
	"math"
	"math/big"
	"strconv"
	"sync/atomic"
)

// Equivalence keys name a number by Cypher equality: two numbers have one
// key exactly when they are equal, whatever their Go type. An integer is "I"
// and its decimal digits; a whole float has the key of the integer with its
// exact value (so 1.0 ≡ 1 and -0.0 ≡ 0); any other float is "F" and its
// shortest decimal form; and each NaN has a key of its own, as NaN equals
// nothing. The Append forms write into dst, so a caller with a buffer of its
// own allocates nothing.

// nanKeys numbers NaN keys, so no two NaNs share one.
var nanKeys atomic.Uint64

// NextNaNKey is a number no other NaN key has had.
func NextNaNKey() uint64 {
	return nanKeys.Add(1)
}

// AppendIntegerEquivalenceKey appends a signed integer's equivalence key.
func AppendIntegerEquivalenceKey(dst []byte, number int64) []byte {
	return strconv.AppendInt(append(dst, 'I'), number, 10)
}

// AppendUnsignedEquivalenceKey appends an unsigned integer's equivalence key.
func AppendUnsignedEquivalenceKey(dst []byte, number uint64) []byte {
	return strconv.AppendUint(append(dst, 'I'), number, 10)
}

// AppendFloatEquivalenceKey appends a float's equivalence key.
func AppendFloatEquivalenceKey(dst []byte, number float64) []byte {
	switch {
	case math.IsNaN(number):
		return strconv.AppendUint(append(dst, "FNaN#"...), NextNaNKey(), 10)
	case math.IsInf(number, 0) || number != math.Trunc(number):
		return strconv.AppendFloat(append(dst, 'F'), number, 'g', -1, 64)
	case number >= math.MinInt64 && number < math.MaxInt64:
		return AppendIntegerEquivalenceKey(dst, int64(number))
	default:
		return append(append(dst, 'I'), new(big.Float).SetFloat64(number).Text('f', 0)...)
	}
}

// IntegerEquivalenceKey is a signed integer's equivalence key.
func IntegerEquivalenceKey(number int64) string {
	return string(AppendIntegerEquivalenceKey(nil, number))
}

// UnsignedEquivalenceKey is an unsigned integer's equivalence key.
func UnsignedEquivalenceKey(number uint64) string {
	return string(AppendUnsignedEquivalenceKey(nil, number))
}

// FloatEquivalenceKey is a float's equivalence key.
func FloatEquivalenceKey(number float64) string {
	return string(AppendFloatEquivalenceKey(nil, number))
}
