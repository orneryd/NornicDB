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
	"strconv"
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
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case int32:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint64:
		return float64(val), true
	case uint32:
		return float64(val), true
	case string:
		// Use strconv.ParseFloat - handles scientific notation, NaN, Inf
		if f, err := strconv.ParseFloat(val, 64); err == nil {
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
//   - int, int32 (converted)
//   - uint, uint32, uint64 (converted, may overflow for large uint64)
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
	switch val := v.(type) {
	case int64:
		return val, true
	case int:
		return int64(val), true
	case int32:
		return int64(val), true
	case uint:
		return int64(val), true
	case uint32:
		return int64(val), true
	case uint64:
		return int64(val), true
	case float64:
		return int64(val), true
	case float32:
		return int64(val), true
	case string:
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			return i, true
		}
		// Try parsing as float then converting
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return int64(f), true
		}
	}
	return 0, false
}
