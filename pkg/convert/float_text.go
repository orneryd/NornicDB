package convert

import (
	"bytes"
	"math"
	"strconv"
)

// FormatFloat writes a float as Neo4j does (Java's Double.toString): plain
// decimals with at least one fractional digit from 10^-3 up to 10^7
// ("0.001", "9999999.0"), scientific notation with an upper-case E outside
// that range ("1.0E7", "1.23456789E-4"), and "Infinity", "-Infinity" and
// "NaN". bits is the value's precision (32 or 64), so a float32 keeps its
// shortest text. It is the one text of a float: toString(), point text and
// the HTTP API's numbers use it.
func FormatFloat(value float64, bits int) string {
	if math.IsInf(value, 1) {
		return "Infinity"
	}
	if math.IsInf(value, -1) {
		return "-Infinity"
	}
	if math.IsNaN(value) {
		return "NaN"
	}
	var buffer [40]byte
	return string(appendFloatText(buffer[:0], value, bits))
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
	// digits without leading zeros.
	var buffer [32]byte
	scientific := strconv.AppendFloat(buffer[:0], value, 'e', -1, bits)
	marker := bytes.IndexByte(scientific, 'e')
	start := len(dst)
	dst = append(dst, scientific[:marker]...)
	if bytes.IndexByte(dst[start:], '.') < 0 {
		dst = append(dst, '.', '0')
	}
	dst = append(dst, 'E')
	exponent := scientific[marker+1:]
	if exponent[0] == '-' {
		dst = append(dst, '-')
	}
	exponent = bytes.TrimLeft(exponent[1:], "0")
	return append(dst, exponent...)
}
