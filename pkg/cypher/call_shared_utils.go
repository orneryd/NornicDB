package cypher

import (
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/util"
)

// splitTopLevelComma splits a comma-separated string while respecting nested
// (), [], {} groups and quoted strings.
func splitTopLevelComma(input string) []string {
	if strings.TrimSpace(input) == "" {
		return nil
	}

	var parts []string
	var current strings.Builder
	inSingle := false
	inDouble := false
	depth := 0

	for i, r := range input {
		switch r {
		case '\'':
			if !inDouble && (i == 0 || input[i-1] != '\\') {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle && (i == 0 || input[i-1] != '\\') {
				inDouble = !inDouble
			}
		case '(', '[', '{':
			if !inSingle && !inDouble {
				depth++
			}
		case ')', ']', '}':
			if !inSingle && !inDouble && depth > 0 {
				depth--
			}
		case ',':
			if !inSingle && !inDouble && depth == 0 {
				parts = append(parts, strings.TrimSpace(current.String()))
				current.Reset()
				continue
			}
		}
		current.WriteRune(r)
	}

	if s := strings.TrimSpace(current.String()); s != "" {
		parts = append(parts, s)
	}
	return parts
}

func firstPresent(m map[string]interface{}, keys ...string) interface{} {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			return v
		}
	}
	return nil
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
	switch t := v.(type) {
	case int:
		return t, true
	case int64:
		return util.SafeInt64ToInt(t)
	case float64:
		return util.SafeFloat64ToInt(t)
	case string:
		i, err := strconv.Atoi(strings.TrimSpace(t))
		return i, err == nil
	default:
		return 0, false
	}
}

func ragToFloat64(v interface{}) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(t), 64)
		return f, err == nil
	default:
		return 0, false
	}
}

func toFloat32(v interface{}) (float32, bool) {
	f, ok := ragToFloat64(v)
	return float32(f), ok
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
