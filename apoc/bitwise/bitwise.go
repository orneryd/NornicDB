// Package bitwise provides APOC bitwise operations.
//
// This package implements all apoc.bitwise.* functions for bitwise
// operations on integers.
package bitwise

// Op performs a bitwise operation on two integers.
//
// Example:
//
//	apoc.bitwise.op(12, '&', 10) => 8
func Op(a int64, operation string, b int64) int64 {
	switch operation {
	case "&", "AND":
		return a & b
	case "|", "OR":
		return a | b
	case "^", "XOR":
		return a ^ b
	case "<<", "LEFT_SHIFT":
		return a << uint(b)
	case ">>", "RIGHT_SHIFT":
		return a >> uint(b)
	default:
		return 0
	}
}

// And performs bitwise AND.
//
// Example:
//
//	apoc.bitwise.and(12, 10) => 8
func And(values ...int64) int64 {
	if len(values) == 0 {
		return 0
	}

	result := values[0]
	for i := 1; i < len(values); i++ {
		result &= values[i]
	}
	return result
}

// Or performs bitwise OR.
//
// Example:
//
//	apoc.bitwise.or(12, 10) => 14
func Or(values ...int64) int64 {
	if len(values) == 0 {
		return 0
	}

	result := values[0]
	for i := 1; i < len(values); i++ {
		result |= values[i]
	}
	return result
}

// Xor performs bitwise XOR.
//
// Example:
//
//	apoc.bitwise.xor(12, 10) => 6
func Xor(values ...int64) int64 {
	if len(values) == 0 {
		return 0
	}

	result := values[0]
	for i := 1; i < len(values); i++ {
		result ^= values[i]
	}
	return result
}

// Not performs bitwise NOT.
//
// Example:
//
//	apoc.bitwise.not(12) => -13
func Not(value int64) int64 {
	return ^value
}

// LeftShift performs left bit shift.
//
// Example:
//
//	apoc.bitwise.leftShift(5, 2) => 20
func LeftShift(value int64, positions int64) int64 {
	return value << uint(positions)
}

// RightShift performs right bit shift.
//
// Example:
//
//	apoc.bitwise.rightShift(20, 2) => 5
func RightShift(value int64, positions int64) int64 {
	return value >> uint(positions)
}

// SetBit sets a specific bit to 1.
//
// Example:
//
//	apoc.bitwise.setBit(8, 0) => 9
func SetBit(value int64, position int) int64 {
	return value | (1 << uint(position))
}

// ClearBit sets a specific bit to 0.
//
// Example:
//
//	apoc.bitwise.clearBit(9, 0) => 8
func ClearBit(value int64, position int) int64 {
	return value &^ (1 << uint(position))
}

// ToggleBit toggles a specific bit.
//
// Example:
//
//	apoc.bitwise.toggleBit(8, 0) => 9
func ToggleBit(value int64, position int) int64 {
	return value ^ (1 << uint(position))
}

// TestBit tests if a specific bit is set.
//
// Example:
//
//	apoc.bitwise.testBit(9, 0) => true
func TestBit(value int64, position int) bool {
	return (value & (1 << uint(position))) != 0
}

// CountBits counts the number of set bits (population count).
//
// Example:
//
//	apoc.bitwise.countBits(15) => 4
func CountBits(value int64) int {
	count := 0
	for value != 0 {
		count += int(value & 1)
		value >>= 1
	}
	return count
}

// ReverseBits reverses the bits in a value.
//
// Example:
//
//	apoc.bitwise.reverseBits(1) => 9223372036854775808
func ReverseBits(value int64) int64 {
	var result int64
	for i := 0; i < 64; i++ {
		result = (result << 1) | (value & 1)
		value >>= 1
	}
	return result
}

// RotateLeft rotates bits to the left.
//
// Example:
//
//	apoc.bitwise.rotateLeft(1, 2) => 4
func RotateLeft(value int64, positions int) int64 {
	positions = positions % 64
	return (value << uint(positions)) | (value >> uint(64-positions))
}

// RotateRight rotates bits to the right.
//
// Example:
//
//	apoc.bitwise.rotateRight(4, 2) => 1
func RotateRight(value int64, positions int) int64 {
	positions = positions % 64
	return (value >> uint(positions)) | (value << uint(64-positions))
}
