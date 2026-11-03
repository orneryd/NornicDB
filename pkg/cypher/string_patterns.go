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
func SplitByKeyword(ctx context.Context, s, keyword string) ([]string, error) {
	return splitKeywordParts(ctx, s, keyword, keywordSplitGeneric)
}

// SplitByMatch splits by "MATCH " keyword. Convenience wrapper for hot path.
func SplitByMatch(ctx context.Context, s string) ([]string, error) {
	return SplitByKeyword(ctx, s, "MATCH")
}

// SplitByCreate splits by "CREATE " keyword. Convenience wrapper for hot path.
func SplitByCreate(ctx context.Context, s string) ([]string, error) {
	return splitKeywordParts(ctx, s, "CREATE", keywordSplitCreate)
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
func ExtractLimit(ctx context.Context, query string) (int, bool, error) {
	_, value, found, err := decimalAfterKeyword(ctx, query, "LIMIT")
	return value, found, err
}

// ExtractSkip extracts the SKIP value from a query string.
// Returns the value and true if found, or 0 and false if not found.
//
// Example:
//
//	ExtractSkip("MATCH (n) RETURN n SKIP 5 LIMIT 10")
//	// Returns: 5, true
func ExtractSkip(ctx context.Context, query string) (int, bool, error) {
	_, value, found, err := decimalAfterKeyword(ctx, query, "SKIP")
	return value, found, err
}

// ExtractLimitString extracts the LIMIT value as a string (for compatibility).
// Returns empty string if not found.
func ExtractLimitString(ctx context.Context, query string) (string, error) {
	text, _, _, err := decimalAfterKeyword(ctx, query, "LIMIT")
	return text, err
}

// ExtractSkipString extracts the SKIP value as a string (for compatibility).
// Returns empty string if not found.
func ExtractSkipString(ctx context.Context, query string) (string, error) {
	text, _, _, err := decimalAfterKeyword(ctx, query, "SKIP")
	return text, err
}

// decimalAfterKeyword owns the shared decimal extent and conversion used by
// the public LIMIT/SKIP observations. Text is a slice of the original source.
func decimalAfterKeyword(ctx context.Context, s, keyword string) (text string, value int, found bool, err error) {
	idx, err := FindKeywordIndex(ctx, s, keyword)
	if err != nil {
		return "", 0, false, err
	}
	if idx < 0 {
		return "", 0, false, nil
	}
	start, _ := cyphertext.GapEnd(s, idx+len(keyword))
	token, ok := cyphertext.ScanNumber(s, start)
	if !ok || token.Invalid || token.Base != 10 || token.Fraction || token.Exponent || token.Grouped || token.Suffix != 0 {
		return "", 0, false, nil
	}
	for i := start; i < token.End; i++ {
		value = value*10 + int(s[i]-'0')
	}
	if err := ctx.Err(); err != nil {
		return "", 0, false, err
	}
	return s[start:token.End], value, true, nil
}

// =============================================================================
// Keyword Index Finding (for compound query detection)
// =============================================================================

// FindKeywordIndex finds the position of a keyword in a query (case-insensitive).
// Returns -1 if not found. Respects word boundaries.
// This is faster than using regexp for simple keyword detection.
func FindKeywordIndex(ctx context.Context, s, keyword string) (int, error) {
	if err := ctx.Err(); err != nil {
		return -1, err
	}
	opts := defaultKeywordScanOpts()
	opts.SkipParens, opts.SkipBrackets, opts.SkipBraces = false, false, false
	index := keywordIndexFrom(ctx, s, keyword, 0, opts)
	if err := ctx.Err(); err != nil {
		return -1, err
	}
	return index, nil
}

// ContainsKeyword checks if a query contains a keyword (case-insensitive).
// Respects word boundaries.
func ContainsKeyword(ctx context.Context, s, keyword string) (bool, error) {
	index, err := FindKeywordIndex(ctx, s, keyword)
	return index >= 0 && err == nil, err
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
	expr = strings.TrimSpace(expr)
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
	content := strings.TrimSpace(expr[openParen+1 : closeParen])
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

	argumentSource := cyphertext.ReadFunctionArgumentSource(content, cyphertext.DistinctArgumentModifier, distinctFollowsAsName)
	content = content[argumentSource.Arguments.Start:argumentSource.Arguments.End]
	result.Distinct = argumentSource.Modifier == cyphertext.DistinctArgumentModifier

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
func splitKeywordParts(ctx context.Context, s, keyword string, policy keywordSplitPolicy) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == "" || keyword == "" {
		return []string{s}, nil
	}
	opts := defaultKeywordScanOpts()
	opts.SkipParens, opts.SkipBrackets = false, false
	if policy == keywordSplitGeneric {
		opts.SkipBraces = false
	}
	var parts []string
	last := 0
	for from := 0; from < len(s); {
		index := keywordIndexFrom(ctx, s, keyword, from, opts)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
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
		parts = append(parts, s[last:index])
		last = end
		for last < len(s) && cyphertext.IsWhitespaceAt(s, last) {
			last++
		}
		from = last
	}
	return append(parts, s[last:]), nil
}
