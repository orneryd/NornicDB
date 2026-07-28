package util

const (
	maxIntValue        = int(^uint(0) >> 1)
	MaxPreallocHintCap = 1 << 16
)

// SafeIntProduct multiplies two positive ints and reports whether the result fits in int.
func SafeIntProduct(a, b int) (int, bool) {
	if a < 0 || b < 0 {
		return 0, false
	}
	if a == 0 || b == 0 {
		return 0, true
	}
	if a > maxIntValue/b {
		return 0, false
	}
	return a * b, true
}

// SafeIntAdd adds two non-negative ints and reports whether the result fits in int.
func SafeIntAdd(a, b int) (int, bool) {
	if a < 0 || b < 0 {
		return 0, false
	}
	if a > maxIntValue-b {
		return 0, false
	}
	return a + b, true
}

// SafeIntSum adds non-negative ints and reports whether the result fits in int.
func SafeIntSum(values ...int) (int, bool) {
	total := 0
	for _, value := range values {
		next, ok := SafeIntAdd(total, value)
		if !ok {
			return 0, false
		}
		total = next
	}
	return total, true
}

// SafePreallocCap bounds slice capacity hints by caller-provided limits and a hard ceiling.
// It is intended for make(..., 0, cap) sites where correctness does not depend on the exact hint.
func SafePreallocCap(requested int, bounds ...int) int {
	if requested <= 0 {
		return 0
	}
	capHint := requested
	for _, bound := range bounds {
		if bound < 0 {
			continue
		}
		if capHint > bound {
			capHint = bound
		}
	}
	if capHint > MaxPreallocHintCap {
		capHint = MaxPreallocHintCap
	}
	return capHint
}

// SafePreallocProduct multiplies two non-negative ints for a capacity hint and clamps the result.
func SafePreallocProduct(a, b int, bounds ...int) int {
	if product, ok := SafeIntProduct(a, b); ok {
		return SafePreallocCap(product, bounds...)
	}
	return SafePreallocCap(maxIntValue, bounds...)
}

// SafePreallocSum adds two non-negative ints for a capacity hint and clamps the result.
func SafePreallocSum(a, b int, bounds ...int) int {
	if sum, ok := SafeIntAdd(a, b); ok {
		return SafePreallocCap(sum, bounds...)
	}
	return SafePreallocCap(maxIntValue, bounds...)
}
