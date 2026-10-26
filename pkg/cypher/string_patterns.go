// Package cypher - Optimized string-based pattern matching for hot paths.
//
// This file provides fast string-based alternatives to regex patterns for
// operations that are called on every query. These functions are 5-10x faster
// than their regex equivalents.
//
// Performance comparison (benchmark on M1 Mac):
//   - extractLimitSkip vs regex FindStringSubmatch: ~6x faster
//   - extractParameter vs regex FindAllStringSubmatch: ~5x faster
package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"
)

// =============================================================================
// Keyword Splitting (replaces matchKeywordPattern and createKeywordPattern)
// =============================================================================

// SplitByKeyword splits a string by a keyword (case-insensitive), respecting word boundaries.
// This is ~8x faster than regexp.MustCompile(`(?i)\bKEYWORD\s+`).Split().
//
// Example:
//
//	SplitByKeyword("MATCH (a) MATCH (b)", "MATCH")
//	// Returns: ["", "(a) ", "(b)"]
func SplitByKeyword(s, keyword string) []string {
	return splitKeywordParts(braceView{source: cyphertext.NewSource(context.Background(), s, cyphertext.AcceptedSource)}, keyword, keywordSplitGeneric)
}

// SplitByMatch splits by "MATCH " keyword. Convenience wrapper for hot path.
func SplitByMatch(s string) []string {
	return SplitByKeyword(s, "MATCH")
}

// SplitByCreate splits by "CREATE " keyword. Convenience wrapper for hot path.
func SplitByCreate(s string) []string {
	return splitKeywordParts(braceView{source: cyphertext.NewSource(context.Background(), s, cyphertext.AcceptedSource)}, "CREATE", keywordSplitCreate)
}

// =============================================================================
// LIMIT/SKIP Extraction (replaces limitPattern and skipPattern)
// =============================================================================

// ExtractLimit extracts the LIMIT value from a query string.
// Returns the value and true if found, or 0 and false if not found.
// This is ~6x faster than regex FindStringSubmatch.
//
// Example:
//
//	ExtractLimit("MATCH (n) RETURN n LIMIT 10")
//	// Returns: 10, true
func ExtractLimit(query string) (int, bool) {
	_, value, found := decimalAfterKeyword(braceView{source: cyphertext.NewSource(context.Background(), query, cyphertext.AcceptedSource)}, "LIMIT")
	return value, found
}

// ExtractSkip extracts the SKIP value from a query string.
// Returns the value and true if found, or 0 and false if not found.
//
// Example:
//
//	ExtractSkip("MATCH (n) RETURN n SKIP 5 LIMIT 10")
//	// Returns: 5, true
func ExtractSkip(query string) (int, bool) {
	_, value, found := decimalAfterKeyword(braceView{source: cyphertext.NewSource(context.Background(), query, cyphertext.AcceptedSource)}, "SKIP")
	return value, found
}

// ExtractLimitString extracts the LIMIT value as a string (for compatibility).
// Returns empty string if not found.
func ExtractLimitString(query string) string {
	text, _, _ := decimalAfterKeyword(braceView{source: cyphertext.NewSource(context.Background(), query, cyphertext.AcceptedSource)}, "LIMIT")
	return text
}

// ExtractSkipString extracts the SKIP value as a string (for compatibility).
// Returns empty string if not found.
func ExtractSkipString(query string) string {
	text, _, _ := decimalAfterKeyword(braceView{source: cyphertext.NewSource(context.Background(), query, cyphertext.AcceptedSource)}, "SKIP")
	return text
}

// decimalAfterKeyword owns the shared decimal extent and conversion used by
// the public LIMIT/SKIP observations. Text is a slice of the original source.
func decimalAfterKeyword(view braceView, keyword string) (text string, value int, found bool) {
	s := view.text()
	idx := findKeywordView(view, keyword)
	if idx < 0 {
		return "", 0, false
	}
	start, _ := cyphertext.GapEnd(s, idx+len(keyword))
	token, ok := cyphertext.ScanNumber(s, start)
	if !ok || token.Invalid || token.Base != 10 || token.Fraction || token.Exponent || token.Grouped || token.Suffix != 0 {
		return "", 0, false
	}
	for i := start; i < token.End; i++ {
		value = value*10 + int(s[i]-'0')
	}
	return s[start:token.End], value, true
}

// =============================================================================
// Keyword Index Finding (for compound query detection)
// =============================================================================

// FindKeywordIndex finds the position of a keyword in a query (case-insensitive).
// Returns -1 if not found. Respects word boundaries.
// This is faster than using regexp for simple keyword detection.
func FindKeywordIndex(s, keyword string) int {
	// Exported string API: this argument is a fresh standalone input.
	return findKeywordView(braceView{source: cyphertext.NewSource(context.Background(), s, cyphertext.AcceptedSource)}, keyword)
}

func findKeywordView(view braceView, keyword string) int {
	s := view.text()
	opts := defaultKeywordScanOpts(view)
	opts.SkipParens, opts.SkipBrackets, opts.SkipBraces = false, false, false
	return keywordIndexFrom(s, keyword, 0, opts)
}

// ContainsKeyword checks if a query contains a keyword (case-insensitive).
// Respects word boundaries.
func ContainsKeyword(s, keyword string) bool {
	return FindKeywordIndex(s, keyword) >= 0
}

// =============================================================================
// Aggregation Function Parsing (replaces 8 separate regex patterns)
// =============================================================================

// AggregationResult holds the parsed components of an aggregation expression.
type AggregationResult struct {
	Function string // COUNT, SUM, AVG, MIN, MAX, COLLECT
	Variable string // The variable name (e.g., "n")
	Property string // The property name (e.g., "age"), empty for COUNT(n) or COUNT(*)
	Distinct bool   // True if DISTINCT was specified
	IsStar   bool   // True if COUNT(*)
}

// ParseAggregation parses an aggregation expression like "COUNT(n.prop)" or "SUM(DISTINCT n.age)".
// This replaces 8 separate regex patterns with one unified parser (~5x faster).
//
// Returns nil if the expression is not a valid aggregation.
//
// Example:
//
//	ParseAggregation("COUNT(n.age)") → {Function: "COUNT", Variable: "n", Property: "age"}
//	ParseAggregation("SUM(DISTINCT x.value)") → {Function: "SUM", Variable: "x", Property: "value", Distinct: true}
//	ParseAggregation("COUNT(*)") → {Function: "COUNT", IsStar: true}
func ParseAggregation(expr string) *AggregationResult {
	source := cyphertext.NewSource(context.Background(), expr, cyphertext.AcceptedSource)
	return parseAggregation(sourceBraceView(source))
}

func parseAggregation(view braceView) *AggregationResult {
	view = view.trim()
	expr := view.text()
	if len(expr) < 5 { // Minimum: "MIN()"
		return nil
	}

	upper := upperASCII(expr)

	// Find the function name
	var funcName string
	var funcLen int
	for _, fn := range []string{"COLLECT", "COUNT", "SUM", "AVG", "MIN", "MAX"} {
		if strings.HasPrefix(upper, fn+"(") {
			funcName = fn
			funcLen = len(fn)
			break
		}
	}
	if funcName == "" {
		return nil
	}

	// Find the opening and closing parentheses
	openParen := funcLen
	if expr[openParen] != '(' {
		return nil
	}

	// Find matching closing paren
	closeParen := len(expr) - 1
	for closeParen > openParen && expr[closeParen] != ')' {
		closeParen--
	}
	if closeParen <= openParen {
		return nil
	}

	// Extract the content inside parentheses
	contentView := view.window(openParen+1, closeParen).trim()
	content := contentView.text()
	if content == "" {
		return nil
	}

	result := &AggregationResult{
		Function: funcName,
	}

	// Check for COUNT(*)
	if content == "*" {
		result.IsStar = true
		return result
	}

	argumentView, modifier := contentView.argumentSource(cyphertext.DistinctArgumentModifier)
	content = argumentView.text()
	result.Distinct = modifier == cyphertext.DistinctArgumentModifier

	// Parse variable.property or just variable
	dotIdx := strings.Index(content, ".")
	if dotIdx > 0 {
		// Has property: variable.property
		varPart := content[:dotIdx]
		propPart := content[dotIdx+1:]

		if !cyphertext.IsIdentifier(varPart) || !cyphertext.IsIdentifier(propPart) {
			return nil
		}

		result.Variable = varPart
		result.Property = propPart
	} else {
		// Just variable: COUNT(n) or similar
		if !cyphertext.IsIdentifier(content) {
			return nil
		}
		result.Variable = content
	}

	return result
}

// isValidIdentifier checks if s is a valid Cypher identifier (alphanumeric + underscore, starts with letter/underscore).

// ParseAggregationProperty is a convenience function that returns just the variable and property.
// Returns ("", "") if not a valid aggregation with a property.
// This provides compatibility with the regex match[1], match[2] pattern.
func ParseAggregationProperty(expr string) (variable, property string) {
	result := ParseAggregation(expr)
	if result == nil {
		return "", ""
	}
	return result.Variable, result.Property
}

// =============================================================================
// Parameter Extraction (replaces parameterPattern regex)
// =============================================================================

// ExtractParameters finds all parameter references ($name) in a query string.
// Returns a slice of parameter names (without the $ prefix).
// This is ~5x faster than regex FindAllStringSubmatch.
//
// Example:
//
//	ExtractParameters("MATCH (n) WHERE n.name = $name AND n.age > $minAge")
//	// Returns: ["name", "minAge"]
func ExtractParameters(query string) []string {
	var names []string
	cyphertext.ParameterReferences(query, func(name string, _, _ int) { names = append(names, name) })
	return names
}

// ReplaceParameters replaces all parameter references with their values.
// The replacer function receives the parameter name (without $) and returns the replacement string.
// This is ~5x faster than regex ReplaceAllStringFunc.
//
// Example:
//
//	ReplaceParameters("WHERE n.name = $name", func(param string) string {
//	    return fmt.Sprintf("'%s'", params[param])
//	})
func ReplaceParameters(query string, replacer func(paramName string) string) string {
	return replaceParameterReferences(query, func(name string, _ byte) (string, bool) {
		return replacer(name), true
	})
}

type keywordSplitPolicy uint8

const (
	keywordSplitGeneric keywordSplitPolicy = iota
	keywordSplitCreate
)

// splitKeywordParts owns slicing and whitespace trimming for both public policies.
func splitKeywordParts(view braceView, keyword string, policy keywordSplitPolicy) []string {
	s := view.text()
	var parts []string
	visitKeywordParts(view, keyword, policy, func(span cyphertext.Span) { parts = append(parts, s[span.Start:span.End]) })
	return parts
}

// Author: #1663 lane. Both the public text adapter and carried-view consumers
// use this existing splitter's extents; there is one keyword/slicing policy.
func visitKeywordParts(view braceView, keyword string, policy keywordSplitPolicy, visit func(cyphertext.Span)) {
	s := view.text()
	if s == "" || keyword == "" {
		visit(cyphertext.Span{End: len(s)})
		return
	}
	opts := defaultKeywordScanOpts(view)
	opts.SkipParens, opts.SkipBrackets = false, false
	if policy == keywordSplitGeneric {
		opts.SkipBraces = false
	}
	last := 0
	for from := 0; from < len(s); {
		index := keywordIndexFrom(s, keyword, from, opts)
		if index < 0 {
			break
		}
		end := index + len(keyword)
		if policy == keywordSplitGeneric {
			var ok bool
			end, ok = keywordMatchAt(s, index, keyword, 0, len(keyword))
			if !ok {
				break
			}
		}
		from = end
		if end == len(s) || !cyphertext.IsWhitespaceAt(s, end) {
			if policy == keywordSplitCreate {
				break
			}
			continue
		}
		visit(cyphertext.Span{Start: last, End: index})
		last = end
		for last < len(s) && cyphertext.IsWhitespaceAt(s, last) {
			last++
		}
		from = last
	}
	visit(cyphertext.Span{Start: last, End: len(s)})
}
