package util

import (
	"fmt"
	"math"
	"strconv"
)

const minIntValue = -maxIntValue - 1

// SafeInt64ToInt converts int64 to int when the value fits.
func SafeInt64ToInt(v int64) (int, bool) {
	converted, err := strconv.Atoi(strconv.FormatInt(v, 10))
	return converted, err == nil
}

// ClampInt64ToInt converts int64 to int and saturates on overflow.
func ClampInt64ToInt(v int64) int {
	if out, ok := SafeInt64ToInt(v); ok {
		return out
	}
	if v < 0 {
		return minIntValue
	}
	return maxIntValue
}

// SafeFloat64ToInt converts float64 to int using truncation toward zero when
// the truncated value is finite and fits in int.
func SafeFloat64ToInt(v float64) (int, bool) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, false
	}
	truncated := math.Trunc(v)
	if strconv.IntSize == 32 {
		if truncated < math.MinInt32 || truncated > math.MaxInt32 {
			return 0, false
		}
	} else if truncated < math.MinInt64 || truncated > math.MaxInt64 {
		return 0, false
	}
	return int(truncated), true
}

// SafeFloat64ToInt64 converts float64 to int64 using truncation toward zero
// when the truncated value is finite and fits in int64.
func SafeFloat64ToInt64(v float64) (int64, bool) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, false
	}
	truncated := math.Trunc(v)
	if truncated < float64(math.MinInt64) || truncated > float64(math.MaxInt64) {
		return 0, false
	}
	return int64(truncated), true
}

// SafeIntToInt32 converts int to int32 when the value fits.
func SafeIntToInt32(v int) (int32, bool) {
	if v < math.MinInt32 || v > math.MaxInt32 {
		return 0, false
	}
	return int32(v), true
}

// SafeInt32ToInt converts int32 to int.
func SafeInt32ToInt(v int32) (int, bool) {
	return int(v), true
}

// SafeIntToUint32 converts a non-negative int to uint32 when it fits.
func SafeIntToUint32(v int) (uint32, bool) {
	var parsed uint32
	if _, err := fmt.Sscan(strconv.Itoa(v), &parsed); err != nil {
		return 0, false
	}
	return parsed, true
}

// SafeIntToUint16 converts a non-negative int to uint16 when it fits.
func SafeIntToUint16(v int) (uint16, bool) {
	var parsed uint16
	if _, err := fmt.Sscan(strconv.Itoa(v), &parsed); err != nil {
		return 0, false
	}
	return parsed, true
}
