// Package text implements Cypher string operations in Unicode code points.
package text

import "unicode/utf8"

// Length returns the number of Unicode code points in value.
func Length(value string) int {
	return utf8.RuneCountInString(value)
}

// Substring returns at most length Unicode code points beginning at start.
// Negative starts retain the evaluators' historical clamp-to-zero behavior;
// callers that expose Cypher argument errors validate before this helper.
func Substring(value string, start, length int) string {
	if length < 0 {
		return ""
	}
	if start < 0 {
		start = 0
	}
	runes := []rune(value)
	if start >= len(runes) {
		return ""
	}
	end := start + length
	if end < start || end > len(runes) {
		end = len(runes)
	}
	return string(runes[start:end])
}

// From returns the suffix beginning at the zero-based Unicode code-point index.
func From(value string, start int) string {
	if start < 0 {
		start = 0
	}
	runes := []rune(value)
	if start >= len(runes) {
		return ""
	}
	return string(runes[start:])
}

// Left returns at most length Unicode code points from the start of value.
func Left(value string, length int) string {
	if length < 0 {
		return ""
	}
	runes := []rune(value)
	if length >= len(runes) {
		return value
	}
	return string(runes[:length])
}

// Right returns at most length Unicode code points from the end of value.
func Right(value string, length int) string {
	if length < 0 {
		return ""
	}
	runes := []rune(value)
	if length >= len(runes) {
		return value
	}
	return string(runes[len(runes)-length:])
}

// At returns the Unicode character at index. Negative indexes count from the
// end, matching Cypher list/string indexing semantics.
func At(value string, index int) (string, bool) {
	runes := []rune(value)
	if index < 0 {
		index += len(runes)
	}
	if index < 0 || index >= len(runes) {
		return "", false
	}
	return string(runes[index]), true
}
