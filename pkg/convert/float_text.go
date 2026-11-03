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
	if token, whole := smallWholeFloatText(value); whole {
		return token
	}
	var buffer [40]byte
	return string(appendFloatText(buffer[:0], value, bits))
}

// smallWholeFloatText reports whether value is a whole FLOAT from 0 through 99
// and returns its canonical text from the immutable spellings in this file.
// A whole FLOAT keeps a fractional token, so its text is longer than one byte:
// strconv answers a small INTEGER from its static string table but cannot
// answer "10.0", so copying appendFloatText's result into a string would
// allocate on every call for a value that is a constant. The spellings are the
// writer's own; TestSmallWholeFloatTextMatchesWriter holds every one of them,
// and negative zero, to appendFloatText's output at both precisions.
func smallWholeFloatText(value float64) (string, bool) {
	if value == 0 {
		if math.Signbit(value) {
			return "-0.0", true
		}
		return "0.0", true
	}
	if value != math.Trunc(value) || value < 1 || value >= 100 {
		return "", false
	}
	if value < 10 {
		switch value {
		case 1:
			return "1.0", true
		case 2:
			return "2.0", true
		case 3:
			return "3.0", true
		case 4:
			return "4.0", true
		case 5:
			return "5.0", true
		case 6:
			return "6.0", true
		case 7:
			return "7.0", true
		case 8:
			return "8.0", true
		case 9:
			return "9.0", true
		}
	}
	start := (int(value) - 10) * 4
	return wholeTensFloatText[start : start+4], true
}

// wholeTensFloatText is the canonical text of the whole FLOATs 10 through 99 in
// ascending order at a fixed width of four bytes ("10.0" through "99.0"), one
// row per ten, so the text of a value is a slice of this single immutable
// 360-byte constant instead of a string copied out of the writer's buffer on
// every call. It is a spelling shortcut, not a second rule: every token equals
// appendFloatText's own output at both precisions.
const wholeTensFloatText = "10.0" + "11.0" + "12.0" + "13.0" + "14.0" + "15.0" + "16.0" + "17.0" + "18.0" + "19.0" +
	"20.0" + "21.0" + "22.0" + "23.0" + "24.0" + "25.0" + "26.0" + "27.0" + "28.0" + "29.0" +
	"30.0" + "31.0" + "32.0" + "33.0" + "34.0" + "35.0" + "36.0" + "37.0" + "38.0" + "39.0" +
	"40.0" + "41.0" + "42.0" + "43.0" + "44.0" + "45.0" + "46.0" + "47.0" + "48.0" + "49.0" +
	"50.0" + "51.0" + "52.0" + "53.0" + "54.0" + "55.0" + "56.0" + "57.0" + "58.0" + "59.0" +
	"60.0" + "61.0" + "62.0" + "63.0" + "64.0" + "65.0" + "66.0" + "67.0" + "68.0" + "69.0" +
	"70.0" + "71.0" + "72.0" + "73.0" + "74.0" + "75.0" + "76.0" + "77.0" + "78.0" + "79.0" +
	"80.0" + "81.0" + "82.0" + "83.0" + "84.0" + "85.0" + "86.0" + "87.0" + "88.0" + "89.0" +
	"90.0" + "91.0" + "92.0" + "93.0" + "94.0" + "95.0" + "96.0" + "97.0" + "98.0" + "99.0"

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
