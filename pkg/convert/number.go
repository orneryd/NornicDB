package convert

import (
	"math"
	"reflect"
)

// NumberKind preserves signed, unsigned and floating identity. In particular,
// comparisons must not narrow an unsigned integer or round an integer to float.
type NumberKind uint8

const (
	NotNumber NumberKind = iota
	SignedNumber
	UnsignedNumber
	FloatNumber
)

// Number is a numeric value without a lossy conversion. bits holds an integer's
// original bits or a float64's IEEE representation; kind determines its reader.
type Number struct {
	bits uint64
	kind NumberKind
}

// ReadNumber is the single runtime numeric-kind reader, including named scalar
// types carried by typed lists. Text and booleans are conversion policies, not
// numeric values.
func ReadNumber(value interface{}) (Number, bool) {
	switch n := value.(type) {
	case int:
		return Number{uint64(n), SignedNumber}, true
	case int8:
		return Number{uint64(n), SignedNumber}, true
	case int16:
		return Number{uint64(n), SignedNumber}, true
	case int32:
		return Number{uint64(n), SignedNumber}, true
	case int64:
		return Number{uint64(n), SignedNumber}, true
	case uint:
		return Number{uint64(n), UnsignedNumber}, true
	case uint8:
		return Number{uint64(n), UnsignedNumber}, true
	case uint16:
		return Number{uint64(n), UnsignedNumber}, true
	case uint32:
		return Number{uint64(n), UnsignedNumber}, true
	case uint64:
		return Number{n, UnsignedNumber}, true
	case float32:
		return Number{math.Float64bits(float64(n)), FloatNumber}, true
	case float64:
		return Number{math.Float64bits(n), FloatNumber}, true
	}
	return ReadReflectNumber(reflect.ValueOf(value))
}

// ReadReflectNumber is the same reader for reflective plugin arguments and
// named scalars; it avoids constructing a boxed copy of a reflect.Value.
func ReadReflectNumber(n reflect.Value) (Number, bool) {
	if n.IsValid() {
		switch n.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return Number{uint64(n.Int()), SignedNumber}, true
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return Number{n.Uint(), UnsignedNumber}, true
		case reflect.Float32, reflect.Float64:
			return Number{math.Float64bits(n.Float()), FloatNumber}, true
		}
	}
	return Number{}, false
}

func (number Number) Kind() NumberKind { return number.kind }

func (number Number) IsInteger() bool {
	return number.kind == SignedNumber || number.kind == UnsignedNumber
}

func IsInteger(value interface{}) bool {
	number, ok := ReadNumber(value)
	return ok && number.IsInteger()
}

func (number Number) Signed() (int64, bool) {
	if number.kind == SignedNumber {
		return int64(number.bits), true
	}
	return 0, false
}

func (number Number) Unsigned() (uint64, bool) {
	if number.kind == UnsignedNumber {
		return number.bits, true
	}
	return 0, false
}

func (number Number) Float() (float64, bool) {
	if number.kind == FloatNumber {
		return math.Float64frombits(number.bits), true
	}
	return 0, false
}

func (number Number) Float64() float64 {
	switch number.kind {
	case SignedNumber:
		return float64(int64(number.bits))
	case UnsignedNumber:
		return float64(number.bits)
	case FloatNumber:
		return math.Float64frombits(number.bits)
	}
	return 0
}

// Float32 checks a finite narrowing before casting. Existing IEEE nonfinite
// inputs remain representable; vector callers validate finiteness separately.
func (number Number) Float32() (float32, bool) {
	if number.kind == NotNumber {
		return 0, false
	}
	floating := number.Float64()
	if !math.IsInf(floating, 0) && (floating > math.MaxFloat32 || floating < -math.MaxFloat32) {
		return 0, false
	}
	return float32(floating), true
}

// NumericFloat64 reads numbers only. It deliberately does not parse strings.
func NumericFloat64(value interface{}) (float64, bool) {
	number, ok := ReadNumber(value)
	if !ok {
		return 0, false
	}
	return number.Float64(), true
}

func SignedInteger(value interface{}) (int64, bool) {
	number, _ := ReadNumber(value)
	return number.Signed()
}

func UnsignedInteger(value interface{}) (uint64, bool) {
	number, _ := ReadNumber(value)
	return number.Unsigned()
}

func FloatValue(value interface{}) (float64, bool) {
	number, _ := ReadNumber(value)
	return number.Float()
}

// IntegerValue recognizes integer kinds only and checks signed representability.
func IntegerValue(value interface{}) (int64, bool) {
	return Int64(value, IntegerOnly)
}
