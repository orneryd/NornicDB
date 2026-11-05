package storage

import (
	"cmp"
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
	order, numeric, ordered := compareConstraintNumbers(a, b)
	return numeric && ordered && order == 0, numeric
}

// compareConstraintNumbers is the exact numeric owner behind constraint
// equality and ordering: integers never widen through float64, so values
// above 2^53 keep their order and identity against each other and against
// floats. numeric is false unless both values are numbers; ordered is false
// when either is NaN. Either sign of zero compares equal.
func compareConstraintNumbers(a, b reflect.Value) (order int, numeric, ordered bool) {
	ak, bk := a.Kind(), b.Kind()
	an := constraintSigned(ak) || constraintUnsigned(ak) || constraintFloat(ak)
	bn := constraintSigned(bk) || constraintUnsigned(bk) || constraintFloat(bk)
	if !an || !bn {
		return 0, false, false
	}
	switch {
	case constraintFloat(ak) && constraintFloat(bk):
		x, y := a.Float(), b.Float()
		if math.IsNaN(x) || math.IsNaN(y) {
			return 0, true, false
		}
		return cmp.Compare(x, y), true, true
	case constraintFloat(ak):
		order, _, ordered = compareConstraintNumbers(b, a)
		return -order, true, ordered
	case constraintSigned(ak) && constraintSigned(bk):
		return cmp.Compare(a.Int(), b.Int()), true, true
	case constraintUnsigned(ak) && constraintUnsigned(bk):
		return cmp.Compare(a.Uint(), b.Uint()), true, true
	case constraintSigned(ak) && constraintUnsigned(bk):
		if a.Int() < 0 {
			return -1, true, true
		}
		return cmp.Compare(uint64(a.Int()), b.Uint()), true, true
	case constraintUnsigned(ak) && constraintSigned(bk):
		if b.Int() < 0 {
			return 1, true, true
		}
		return cmp.Compare(a.Uint(), uint64(b.Int())), true, true
	case constraintSigned(ak):
		order, ordered = compareConstraintIntegerFloat(a.Int(), b.Float())
		return order, true, ordered
	default:
		order, ordered = compareConstraintUnsignedFloat(a.Uint(), b.Float())
		return order, true, ordered
	}
}

// compareConstraintIntegerFloat orders n against f exactly: by f's integral
// part first, then by its fraction.
func compareConstraintIntegerFloat(n int64, f float64) (int, bool) {
	switch {
	case math.IsNaN(f):
		return 0, false
	case f >= 0x1p63:
		return -1, true
	case f < -0x1p63:
		return 1, true
	}
	t := math.Trunc(f)
	if order := cmp.Compare(n, int64(t)); order != 0 {
		return order, true
	}
	return cmp.Compare(t, f), true
}

func compareConstraintUnsignedFloat(n uint64, f float64) (int, bool) {
	switch {
	case math.IsNaN(f):
		return 0, false
	case f >= 0x1p64:
		return -1, true
	case f < 0:
		return 1, true
	}
	t := math.Trunc(f)
	if order := cmp.Compare(n, uint64(t)); order != 0 {
		return order, true
	}
	return cmp.Compare(t, f), true
}

func constraintIntegerFloatEqual(n int64, f float64) bool {
	order, ordered := compareConstraintIntegerFloat(n, f)
	return ordered && order == 0
}
func constraintUnsignedFloatEqual(n uint64, f float64) bool {
	order, ordered := compareConstraintUnsignedFloat(n, f)
	return ordered && order == 0
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
