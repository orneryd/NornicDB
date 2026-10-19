package util

import (
	"encoding/hex"
	"fmt"
	"hash"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// hexDigest is the lower-case hex digest of DigestText(value), the one
// implementation of apoc.util.md5, sha1 and sha256.
func hexDigest(digest hash.Hash, value interface{}) string {
	_, _ = digest.Write([]byte(DigestText(value)))
	return hex.EncodeToString(digest.Sum(nil))
}

// DigestText is the text APOC's hash functions digest, as UTF-8: for a list
// (APOC's argument, LIST<ANY>), the concatenation of its values' Java string
// forms, a null value adding nothing, so apoc.util.sha256(['a', 1, null])
// digests "a1". A value that isn't a list (a NornicDB extension) digests its
// own string form, and null the empty text.
//
// The string forms are Java's for the values APOC receives: a string is
// itself; an integer is in decimal; a float as Java's Double.toString writes
// it (1.0, 1.0E7, 1.0E-4, NaN, Infinity); a boolean true or false; a list
// [a, b] and a map {k=v} with sorted keys. Utils.toHashString renders null
// as empty text at every depth, including list elements and map values.
// Other values are written as
// their Cypher text (fmt.Stringer), which Java's may differ from.
func DigestText(value interface{}) string {
	var out strings.Builder
	switch values := value.(type) {
	case nil:
	case []interface{}:
		for _, item := range values {
			if item != nil {
				writeJavaString(&out, item)
			}
		}
	case []string:
		for _, item := range values {
			out.WriteString(item)
		}
	default:
		if list, ok := digestListValue(value); ok {
			writeJavaList(&out, list, false)
		} else {
			writeJavaString(&out, value)
		}
	}
	return out.String()
}

func writeJavaString(out *strings.Builder, value interface{}) {
	switch v := value.(type) {
	case nil:
	case string:
		out.WriteString(v)
	case bool:
		out.WriteString(strconv.FormatBool(v))
	case int:
		out.WriteString(strconv.FormatInt(int64(v), 10))
	case int32:
		out.WriteString(strconv.FormatInt(int64(v), 10))
	case int64:
		out.WriteString(strconv.FormatInt(v, 10))
	case float32:
		out.WriteString(JavaDoubleString(float64(v)))
	case float64:
		out.WriteString(JavaDoubleString(v))
	case []byte:
		// APOC's Utils.toHashString uses Arrays.toString(byte[]): Java
		// bytes are signed, unlike Go's byte. Keep binary values as bytes.
		out.WriteByte('[')
		var text [4]byte // -128 is the longest signed byte.
		for index, item := range v {
			if index > 0 {
				out.WriteString(", ")
			}
			out.Write(strconv.AppendInt(text[:0], int64(int8(item)), 10))
		}
		out.WriteByte(']')
	case []interface{}:
		out.WriteByte('[')
		for index, item := range v {
			if index > 0 {
				out.WriteString(", ")
			}
			writeJavaString(out, item)
		}
		out.WriteByte(']')
	case []string:
		out.WriteString("[" + strings.Join(v, ", ") + "]")
	case map[string]interface{}:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out.WriteByte('{')
		for index, key := range keys {
			if index > 0 {
				out.WriteString(", ")
			}
			out.WriteString(key)
			out.WriteByte('=')
			writeJavaString(out, v[key])
		}
		out.WriteByte('}')
	default:
		if list, ok := digestListValue(v); ok {
			writeJavaList(out, list, true)
		} else {
			out.WriteString(fmt.Sprint(v))
		}
	}
}

// Cypher lists retain their Go slice/array type across parameter and plugin
// boundaries. Their Java text is independent of that representation.
func digestListValue(value interface{}) (reflect.Value, bool) {
	if _, bytes := value.([]byte); bytes || value == nil {
		return reflect.Value{}, false
	}
	list := reflect.ValueOf(value)
	return list, list.Kind() == reflect.Slice || list.Kind() == reflect.Array
}

func writeJavaList(out *strings.Builder, list reflect.Value, nested bool) {
	if nested {
		out.WriteByte('[')
	}
	for index := 0; index < list.Len(); index++ {
		if nested && index > 0 {
			out.WriteString(", ")
		}
		writeJavaString(out, list.Index(index).Interface())
	}
	if nested {
		out.WriteByte(']')
	}
}

// JavaDoubleString is Java's Double.toString(value) (Java 19 and later,
// shortest round-trip digits): plain notation with at least one fraction
// digit for 1e-3 <= |value| < 1e7 (100.0, 0.001), otherwise computerized
// scientific notation (1.0E7, 1.234E-5); NaN, Infinity, -Infinity; 0.0 and
// -0.0.
func JavaDoubleString(value float64) string {
	switch {
	case math.IsNaN(value):
		return "NaN"
	case math.IsInf(value, 1):
		return "Infinity"
	case math.IsInf(value, -1):
		return "-Infinity"
	case value == 0:
		if math.Signbit(value) {
			return "-0.0"
		}
		return "0.0"
	}
	if magnitude := math.Abs(value); magnitude >= 1e-3 && magnitude < 1e7 {
		text := strconv.FormatFloat(value, 'f', -1, 64)
		if !strings.Contains(text, ".") {
			text += ".0"
		}
		return text
	}
	text := strconv.FormatFloat(value, 'e', -1, 64)
	mantissa, exponent, _ := strings.Cut(text, "e")
	if !strings.Contains(mantissa, ".") {
		// Java's decimal selection includes both one- and two-digit
		// candidates when the shortest has one digit, choosing the nearest
		// (ties to even). Appending .0 to Go's shortest loses this distinction
		// for subnormals: Double.MIN_VALUE is 4.9E-324, not 5.0E-324.
		text = strconv.FormatFloat(value, 'e', 1, 64)
		mantissa, exponent, _ = strings.Cut(text, "e")
	}
	sign := ""
	if exponent[0] == '-' {
		sign = "-"
	}
	return mantissa + "E" + sign + strings.TrimLeft(exponent[1:], "0")
}
