package storage

import (
	"math"
	"reflect"
	"strconv"
	"strings"
)

// Property codecs can return typed slices while query parameters and pending
// writes carry []interface{}. Constraint equality compares their values, not
// their Go container types. Keep the scalar constraint/index contract separate.
func equalConstraintLists(a, b interface{}) (bool, bool) {
	av, bv := reflect.ValueOf(a), reflect.ValueOf(b)
	if !av.IsValid() || !bv.IsValid() || av.Kind() != reflect.Slice || bv.Kind() != reflect.Slice {
		return false, false
	}
	return equalConstraintListValue(av, bv, make(map[constraintListVisit]bool)), true
}

type constraintListVisit struct {
	a, b   uintptr
	length int
}

func equalConstraintListValue(a, b reflect.Value, seen map[constraintListVisit]bool) bool {
	for a.IsValid() && a.Kind() == reflect.Interface {
		a = a.Elem()
	}
	for b.IsValid() && b.Kind() == reflect.Interface {
		b = b.Elem()
	}
	if !a.IsValid() || !b.IsValid() {
		return a.IsValid() == b.IsValid()
	}
	if a.Kind() == reflect.Slice || b.Kind() == reflect.Slice {
		if a.Kind() != reflect.Slice || b.Kind() != reflect.Slice || a.Len() != b.Len() {
			return false
		}
		if a.IsNil() != b.IsNil() {
			return false
		}
		if a.Len() == 0 {
			return true
		}
		// Cyclic Go containers are not persisted property values, but preserving
		// DeepEqual's bounded handling prevents a malformed pending value recursing.
		visit := constraintListVisit{a.Pointer(), b.Pointer(), a.Len()}
		if seen[visit] {
			return true
		}
		seen[visit] = true
		for i := 0; i < a.Len(); i++ {
			if !equalConstraintListValue(a.Index(i), b.Index(i), seen) {
				return false
			}
		}
		return true
	}
	if equal, numeric := equalConstraintListNumbers(a, b); numeric {
		return equal
	}
	if a.Kind() == reflect.String && b.Kind() == reflect.String {
		return a.String() == b.String()
	}
	if a.Kind() == reflect.Bool && b.Kind() == reflect.Bool {
		return a.Bool() == b.Bool()
	}
	return compareValues(a.Interface(), b.Interface())
}

func constraintSigned(k reflect.Kind) bool   { return k >= reflect.Int && k <= reflect.Int64 }
func constraintUnsigned(k reflect.Kind) bool { return k >= reflect.Uint && k <= reflect.Uint64 }
func constraintFloat(k reflect.Kind) bool    { return k == reflect.Float32 || k == reflect.Float64 }

// Compare integral list elements without float64 widening: adjacent int64 values
// above 2^53 must stay distinct, including across typed/generic list boundaries.
func equalConstraintListNumbers(a, b reflect.Value) (bool, bool) {
	ak, bk := a.Kind(), b.Kind()
	an := constraintSigned(ak) || constraintUnsigned(ak) || constraintFloat(ak)
	bn := constraintSigned(bk) || constraintUnsigned(bk) || constraintFloat(bk)
	if !an || !bn {
		return false, false
	}
	if constraintSigned(ak) {
		n := a.Int()
		if constraintSigned(bk) {
			return n == b.Int(), true
		}
		if constraintUnsigned(bk) {
			return n >= 0 && uint64(n) == b.Uint(), true
		}
		return constraintIntegerFloatEqual(n, b.Float()), true
	}
	if constraintUnsigned(ak) {
		n := a.Uint()
		if constraintUnsigned(bk) {
			return n == b.Uint(), true
		}
		if constraintSigned(bk) {
			return b.Int() >= 0 && n == uint64(b.Int()), true
		}
		return constraintUnsignedFloatEqual(n, b.Float()), true
	}
	if constraintSigned(bk) {
		return constraintIntegerFloatEqual(b.Int(), a.Float()), true
	}
	if constraintUnsigned(bk) {
		return constraintUnsignedFloatEqual(b.Uint(), a.Float()), true
	}
	return a.Float() == b.Float(), true // NaN remains unequal; either sign of zero is equal.
}
func constraintIntegerFloatEqual(n int64, f float64) bool {
	return f >= -0x1p63 && f < 0x1p63 && f == math.Trunc(f) && int64(f) == n
}
func constraintUnsignedFloatEqual(n uint64, f float64) bool {
	return f >= 0 && f < 0x1p64 && f == math.Trunc(f) && uint64(f) == n
}

// writeConstraintListNumericKey is the canonical numeric element writer for
// composite/list index keys. Exact integers must not collapse through float64;
// an exactly integral Float uses the same text as the Integer it equals.
func writeConstraintListNumericKey(out *strings.Builder, value interface{}) bool {
	v := reflect.ValueOf(value)
	if !v.IsValid() {
		return false
	}
	k := v.Kind()
	if constraintSigned(k) {
		out.WriteByte('n')
		out.WriteString(strconv.FormatInt(v.Int(), 10))
		return true
	}
	if constraintUnsigned(k) {
		out.WriteByte('n')
		out.WriteString(strconv.FormatUint(v.Uint(), 10))
		return true
	}
	if !constraintFloat(k) {
		return false
	}
	f := v.Float()
	out.WriteByte('n')
	if f >= -0x1p63 && f < 0x1p63 && f == math.Trunc(f) {
		out.WriteString(strconv.FormatInt(int64(f), 10))
	} else if f >= 0 && f < 0x1p64 && f == math.Trunc(f) {
		out.WriteString(strconv.FormatUint(uint64(f), 10))
	} else {
		out.WriteString(strconv.FormatFloat(f, 'g', -1, 64))
	}
	return true
}
