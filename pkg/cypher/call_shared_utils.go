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
	parts := appendTopLevelCommaParts(buffer[:0], input, false)
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

// topLevelCommaIterator is the one value iterator over an expression's
// top-level comma parts. It owns the comma scan: each part is a trimmed
// substring of input together with where that text starts, read from the same
// findTopLevelOperator scan, trimming and comment rule every other top-level
// operator split uses. Quoted names, strings, comments and nested groups
// therefore have one lexical owner, and only the caller's trailing-entry
// policy differs.
//
// The iterator yields expressionFragment values, so a caller keeps either the
// text alone or the text with its start from the same scan, and neither reads
// through an intermediate part list.
type topLevelCommaIterator struct {
	input     string
	start     int
	keepEmpty bool
	done      bool
}

// newTopLevelCommaIterator starts iteration over input's parts outside quotes
// and nested (), [], {} groups. keepEmpty keeps a final empty entry, and an
// input holding only whitespace has no parts either way.
func newTopLevelCommaIterator(input string, keepEmpty bool) topLevelCommaIterator {
	if strings.Contains(input, "/") {
		input = StripComments(input)
	}
	return topLevelCommaIterator{
		input:     input,
		keepEmpty: keepEmpty,
		done:      strings.TrimSpace(input) == "",
	}
}

// next returns the next part and whether the input has one. A part's text is
// trimmed, and its start is where that text begins in the input the iterator
// was given, so two equal parts written at two positions keep two starts.
func (it *topLevelCommaIterator) next() (expressionFragment, bool) {
	if it.done {
		return expressionFragment{}, false
	}
	comma := findTopLevelOperator(it.input, ",", false, true)
	if comma < 0 {
		it.done = true
		last := strings.TrimSpace(it.input)
		if last == "" && !it.keepEmpty {
			return expressionFragment{}, false
		}
		return expressionFragment{text: last, start: it.start + leadingExpressionSpace(it.input)}, true
	}
	part := expressionFragment{
		text:  strings.TrimSpace(it.input[:comma]),
		start: it.start + leadingExpressionSpace(it.input),
	}
	it.input = it.input[comma+1:]
	it.start += comma + 1
	return part, true
}

// appendTopLevelCommaParts is the text owner: it walks the one comma iterator
// and appends each part's text straight into the caller's own buffer. A caller
// that reports source occurrences passes appendExpressionParts instead, which
// appends the iterator's whole part from the same scan.
func appendTopLevelCommaParts(parts []string, input string, keepEmpty bool) []string {
	iterator := newTopLevelCommaIterator(input, keepEmpty)
	for {
		part, ok := iterator.next()
		if !ok {
			return parts
		}
		parts = append(parts, part.text)
	}
}

// appendExpressionParts is the source-span owner: it walks the one comma
// iterator and appends each whole part, text and start, straight into the
// caller's own buffer.
func appendExpressionParts(parts []expressionFragment, input string, keepEmpty bool) []expressionFragment {
	iterator := newTopLevelCommaIterator(input, keepEmpty)
	for {
		part, ok := iterator.next()
		if !ok {
			return parts
		}
		parts = append(parts, part)
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
			if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}
