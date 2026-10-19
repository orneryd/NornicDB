package cypher

import (
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"strconv"
	"strings"
)

// splitTopLevelComma splits a comma-separated string while respecting nested
// (), [], {} groups and quoted strings. The parts are trimmed substrings of
// input, so splitting copies no text.
func splitTopLevelComma(input string) []string {
	var buffer [8]string
	parts := appendTopLevelComma(buffer[:0], input)
	if len(parts) == 0 {
		return nil
	}
	// One allocation of the exact size, as the compiler gives a slice built
	// locally and returned.
	return append([]string(nil), parts...)
}

// appendTopLevelComma appends input's comma-separated parts outside quotes
// and brackets, trimmed, to parts (splitTopLevelComma). A caller passing a
// fixed-size buffer (var buffer [8]string; buffer[:0]) splits without
// allocating.
func appendTopLevelComma(parts []string, input string) []string {
	if strings.TrimSpace(input) == "" {
		return parts
	}

	start := 0
	depth := 0

	for i := 0; i < len(input); i++ {
		switch input[i] {
		case '\'', '"', '`':
			i = skipCypherQuotedText(input, i, input[i]) - 1
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(input[start:i]))
				start = i + 1
			}
		}
	}

	if s := strings.TrimSpace(input[start:]); s != "" {
		parts = append(parts, s)
	}
	return parts
}

func firstPresent(m map[string]interface{}, keys ...string) interface{} {
	value, _ := policyPresent(m, keys...)
	return value
}

func policyPresent(m map[string]interface{}, keys ...string) (interface{}, bool) {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			return v, true
		}
	}
	return nil, false
}

func stringOr(v interface{}, fallback string) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fallback
}

func toBool(v interface{}) (bool, bool) {
	switch t := v.(type) {
	case bool:
		return t, true
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(t))
		return parsed, err == nil
	default:
		return false, false
	}
}

func toInt(v interface{}) (int, bool) {
	return valueconvert.ToInt(v, valueconvert.IntegerFloat|valueconvert.IntegerStrings|valueconvert.IntegerTrimStrings)
}

func ragToFloat64(v interface{}) (float64, bool) {
	if text, ok := v.(string); ok {
		v = strings.TrimSpace(text)
	}
	return valueconvert.ToFloat64(v)
}

func toFloat32(v interface{}) (float32, bool) {
	floating, ok := ragToFloat64(v)
	if !ok {
		return 0, false
	}
	number, numeric := valueconvert.ReadNumber(floating)
	if !numeric {
		return 0, false
	}
	return number.Float32()
}

func toStringSlice(v interface{}) []string {
	switch t := v.(type) {
	case []string:
		return t
	case []interface{}:
		out := make([]string, 0, len(t))
		for _, item := range t {
			if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}
