package qdrantgrpc

import "reflect"

// storedNumber is v as a number when it is one, in whatever integer or float
// width storage returns it.
func storedNumber(v any) (float64, bool) {
	rv := reflect.ValueOf(v)
	switch {
	case rv.CanInt():
		return float64(rv.Int()), true
	case rv.CanUint():
		return float64(rv.Uint()), true
	case rv.CanFloat():
		return rv.Float(), true
	}
	return 0, false
}
