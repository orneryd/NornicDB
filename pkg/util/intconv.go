package util

import (
	"math"
	"strconv"
)

const minIntValue = -maxIntValue - 1

// SafeInt64ToInt converts int64 to int when the value fits.
func SafeInt64ToInt(v int64) (int, bool) {
	if strconv.IntSize == 32 {
		if v < math.MinInt32 || v > math.MaxInt32 {
			return 0, false
		}
	}
	return int(v), true
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

// SafeUint32ToInt converts uint32 to int when the value fits.
func SafeUint32ToInt(v uint32) (int, bool) {
	if uint64(v) > uint64(maxIntValue) {
		return 0, false
	}
	return int(v), true
}

// SafeIntToUint32 converts a non-negative int to uint32 when it fits.
func SafeIntToUint32(v int) (uint32, bool) {
	if v < 0 {
		return 0, false
	}
	if strconv.IntSize > 32 {
		u := uint(v)
		if u>>32 != 0 {
			return 0, false
		}
		return uint32(u), true
	}
	if uint(v)>>32 != 0 {
		return 0, false
	}
	return uint32(v), true
}

// SafeIntToUint16 converts a non-negative int to uint16 when it fits.
func SafeIntToUint16(v int) (uint16, bool) {
	if v < 0 {
		return 0, false
	}
	if uint(v)>>16 != 0 {
		return 0, false
	}
	return uint16(v), true
}
