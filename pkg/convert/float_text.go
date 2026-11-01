// Author: L6 / #1665 lane (Codex gpt-6.1-sol). Keep public numeric text at the existing kind and FLOAT-spelling owners.
package convert

import (
	"bytes"
	"math"
	"reflect"
	"strconv"
)

// NumericText formats a runtime INTEGER or FLOAT without narrowing its value
// or erasing its numeric kind. INTEGERs keep exact decimal digits; FLOATs use
// FormatFloat, including a fractional token for whole values and negative zero.
// A float32 keeps its own shortest text. Non-numeric values are left to the
// caller's existing string, temporal or container representation.
//
// For example, NumericText(int64(2)) returns "2", while
// NumericText(float64(2)) returns "2.0". It does not admit or coerce inputs.
func NumericText(value interface{}) (string, bool) {
	text, _, numeric := numericText(nil, value, true)
	return text, numeric
}

// AppendNumericText appends the same INTEGER or FLOAT token as NumericText to
// dst, without allocating an intermediate string for a surrounding document.
// It leaves dst unchanged and returns false for a non-numeric value.
// For example, AppendNumericText([]byte("value="), float64(2)) appends "2.0".
func AppendNumericText(dst []byte, value interface{}) ([]byte, bool) {
	_, dst, numeric := numericText(dst, value, false)
	return dst, numeric
}

// numericText owns numeric-kind selection for the string and append protocols.
// String output retains strconv's small-integer strings and the nonfinite
// constants; document output writes into its caller's existing buffer.
func numericText(dst []byte, value interface{}, asString bool) (string, []byte, bool) {
	number, numeric := ReadNumber(value)
	if !numeric {
		return "", dst, false
	}
	if integer, signed := number.Signed(); signed {
		if asString {
			return strconv.FormatInt(integer, 10), dst, true
		}
		return "", strconv.AppendInt(dst, integer, 10), true
	}
	if integer, unsigned := number.Unsigned(); unsigned {
		if asString {
			return strconv.FormatUint(integer, 10), dst, true
		}
		return "", strconv.AppendUint(dst, integer, 10), true
	}
	bits := 64
	if reflect.TypeOf(value).Kind() == reflect.Float32 {
		bits = 32
	}
	floating, _ := number.Float()
	if asString {
		return FormatFloat(floating, bits), dst, true
	}
	if special := nonfiniteFloatText(floating); special != "" {
		return "", append(dst, special...), true
	}
	return "", appendFloatText(dst, floating, bits), true
}

// FormatFloat writes a float as Neo4j does (Java's Double.toString): plain
// decimals with at least one fractional digit from 10^-3 up to 10^7
// ("0.001", "9999999.0"), scientific notation with an upper-case E outside
// that range ("1.0E7", "1.23456789E-4"), and "Infinity", "-Infinity" and
// "NaN". bits is the value's precision (32 or 64), so a float32 keeps its
// shortest text. It is the one text of a float: toString(), point text and
// the HTTP API's numbers use it.
func FormatFloat(value float64, bits int) string {
	if special := nonfiniteFloatText(value); special != "" {
		return special
	}
	var buffer [40]byte
	return string(appendFloatText(buffer[:0], value, bits))
}

func nonfiniteFloatText(value float64) string {
	if math.IsInf(value, 1) {
		return "Infinity"
	}
	if math.IsInf(value, -1) {
		return "-Infinity"
	}
	if math.IsNaN(value) {
		return "NaN"
	}
	return ""
}

// appendFloatText is the numeric owner's allocation-free finite FLOAT writer.
// JSON envelopes and the public text formatter share the exact same spelling.
func appendFloatText(dst []byte, value float64, bits int) []byte {
	magnitude := math.Abs(value)
	if magnitude == 0 || magnitude >= 1e-3 && magnitude < 1e7 {
		start := len(dst)
		dst = strconv.AppendFloat(dst, value, 'f', -1, bits)
		if bytes.IndexByte(dst[start:], '.') < 0 {
			dst = append(dst, '.', '0')
		}
		return dst
	}
	// Go writes 1.23456789e+08 or 1e-07: keep the mantissa (with a
	// fractional digit), then E, the exponent's sign when negative and its
	// digits without leading zeros. Java's decimal selection includes both
	// one- and two-digit candidates when the shortest has one digit and picks
	// the nearest, so a one-digit mantissa is re-spelled with two digits:
	// Double.MIN_VALUE is 4.9E-324, not 5.0E-324.
	var buffer [32]byte
	scientific := strconv.AppendFloat(buffer[:0], value, 'e', -1, bits)
	if marker := bytes.IndexByte(scientific, 'e'); bytes.IndexByte(scientific[:marker], '.') < 0 {
		scientific = strconv.AppendFloat(buffer[:0], value, 'e', 1, bits)
	}
	marker := bytes.IndexByte(scientific, 'e')
	dst = append(dst, scientific[:marker]...)
	dst = append(dst, 'E')
	exponent := scientific[marker+1:]
	if exponent[0] == '-' {
		dst = append(dst, '-')
	}
	exponent = bytes.TrimLeft(exponent[1:], "0")
	return append(dst, exponent...)
}
