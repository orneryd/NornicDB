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
	return appendTopLevelCommaParts(parts, input, false)
}

// splitTopLevelCommaKeepEmpty preserves the final empty entry for strict
// grammar consumers, using the same tokens as every other comma reader.
func splitTopLevelCommaKeepEmpty(input string) []string {
	return appendTopLevelCommaParts(nil, input, true)
}

// appendTopLevelCommaParts separates expressions through the canonical token
// reader used for operators. Quoted names, strings, comments and nested groups
// have one lexical owner. Only the caller's trailing-entry policy differs.
func appendTopLevelCommaParts(parts []string, input string, keepEmpty bool) []string {
	if strings.Contains(input, "/") {
		input = StripComments(input)
	}
	if strings.TrimSpace(input) == "" {
		return parts
	}
	for {
		comma := findTopLevelOperator(input, ",", false, true)
		if comma < 0 {
			if last := strings.TrimSpace(input); last != "" || keepEmpty {
				parts = append(parts, last)
			}
			return parts
		}
		parts = append(parts, strings.TrimSpace(input[:comma]))
		input = input[comma+1:]
	}
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
			if s, ok := item.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}
