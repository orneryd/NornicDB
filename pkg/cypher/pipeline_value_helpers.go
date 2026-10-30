package cypher

// Pipeline value helpers parse literals and coerce row values for UNWIND.

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/convert"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// ---- helpers ----

// referencesVariable returns true if the query text refers to the variable
// `name` outside string literals and outside property-access positions.
func referencesVariable(query, name string) bool {
	if name == "" {
		return false
	}
	// Use the existing identifier-aware scanner by replacing with a sentinel
	// and checking for a diff.
	const sentinel = "\x00"
	replaced := replaceIdentifierOutsideQuotes(query, name, sentinel)
	return strings.Contains(replaced, sentinel)
}

// evaluateListForPipeline evaluates a list expression against a binding row.
// Supports three forms:
//  1. Bare variable:  UNWIND items AS x
//  2. Property access: UNWIND row.products AS prodRef
//  3. Literal list:   UNWIND [{...}, {...}] AS x  (already a literal)
//
// A value that isn't a list is one element (coerceToUnwindItems).
//
// Returns nil if the expression can't be evaluated.
func evaluateListForPipeline(expr string, row pipelineRow) []interface{} {
	items, _ := evaluateStaticListForPipeline(expr, row)
	return items
}

// evaluateStaticListForPipeline evaluates an UNWIND list that is a row
// variable, a property of one, or a literal list, without the row evaluator.
// Its value is coerced as UNWIND coerces any value (coerceToUnwindItems).
func evaluateStaticListForPipeline(expr string, row pipelineRow) ([]interface{}, bool) {
	expr = strings.TrimSpace(expr)
	// Bare variable.
	if val, ok := row[expr]; ok {
		return coerceToUnwindItems(val), true
	}
	// Property access (a.b): the whole expression only, not one that starts
	// with it (UNWIND n.p + n.q is a list concatenation).
	if base, field, property := parseVarPropertyRef(expr); property {
		if baseVal, ok := row[base]; ok {
			if asMap, ok := toStringAnyMap(baseVal); ok {
				if v, ok := asMap[field]; ok {
					return coerceToUnwindItems(v), true
				}
			}
			if node, ok := baseVal.(*storage.Node); ok && node != nil {
				return coerceToUnwindItems(node.Properties[field]), true
			}
		}
	}
	// Literal list — parse via an ad-hoc evaluator. The simplest reliable
	// thing to do is wrap the literal and let the storage executor parse it
	// as a value. Without plumbing a full parser here we only accept the
	// `[...]` form and split top-level items.
	if isListLiteral(expr) {
		parsed, ok := parseLiteralValueForPipeline(expr)
		if !ok {
			return nil, false
		}
		return coerceToUnwindItems(parsed), true
	}
	return nil, false
}

func (e *StorageExecutor) evaluateListForPipelineWithContext(ctx context.Context, expr string, row pipelineRow) ([]interface{}, bool) {
	if inner, wrapped := stripEnclosingExpressionParentheses(strings.TrimSpace(expr)); wrapped {
		expr = inner
	}
	if items, ok := evaluateStaticListForPipeline(expr, row); ok {
		return items, true
	}
	value, err := e.checkedPropertyExpression(ctx, expr, row)
	if err != nil {
		return nil, false
	}
	return coerceToUnwindItems(value), true
}

func toAnySlice(v interface{}) []interface{} {
	items, _ := convert.ListValue(v)
	return items
}

func parseLiteralMapForPipeline(s string) map[string]interface{} {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "{") || !strings.HasSuffix(s, "}") {
		return nil
	}
	expressions, err := parsePropertyExpressions(s)
	if err != nil {
		return nil
	}
	out := make(map[string]interface{}, len(expressions))
	for _, expression := range expressions {
		value, literal := parseLiteralValueForPipeline(expression.expr)
		if !literal {
			return nil
		}
		out[expression.key] = value
	}
	return out
}
func parseLiteralListForPipeline(s string) ([]interface{}, bool) {
	s = strings.TrimSpace(s)
	if !isListLiteral(s) {
		return nil, false
	}
	inner := strings.TrimSpace(s[1 : len(s)-1])
	if inner == "" {
		return []interface{}{}, true
	}
	parts := splitTopLevelComma(inner)
	out := make([]interface{}, 0, len(parts))
	for _, part := range parts {
		v, ok := parseLiteralValueForPipeline(part)
		if !ok {
			return nil, false
		}
		out = append(out, v)
	}
	return out, true
}

func parseLiteralValueForPipeline(s string) (interface{}, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, false
	}
	if strings.HasPrefix(s, "{") && strings.HasSuffix(s, "}") {
		m := parseLiteralMapForPipeline(s)
		if m == nil {
			return nil, false
		}
		return m, true
	}
	if isListLiteral(s) {
		return parseLiteralListForPipeline(s)
	}
	return parseLiteralScalarForPipeline(s)
}

func parseLiteralScalarForPipeline(s string) (interface{}, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, false
	}
	// Quoted string.
	if isWholeCypherQuotedString(s) {
		return cyphertext.DecodeString(s, cyphertext.AcceptedSource)
	}
	// true, false, null, NaN, Infinity.
	if value, ok := literalKeywordValue(s); ok {
		return value, true
	}
	// Int.
	if i, ok := parseIntFast(s); ok {
		return i, true
	}
	// Float.
	if f, ok := parseFloatFast(s); ok {
		return f, true
	}
	return nil, false
}

func parseIntFast(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	negative := false
	digits := 0
	if s[0] == '-' {
		negative = true
		digits = 1
	} else if s[0] == '+' {
		digits = 1
	}
	if digits == len(s) {
		return 0, false
	}
	token, ok := cyphertext.ScanNumber(s, digits)
	if !ok || token.Invalid || token.End != len(s) || token.Grouped || token.Fraction || token.Exponent || token.Suffix != 0 {
		return 0, false
	}
	base := token.Base
	digits = token.DigitsStart
	if base == 10 {
		value, err := strconv.ParseInt(s, 10, 64)
		return value, err == nil
	}
	if digits == len(s) {
		return 0, false
	}
	magnitude, err := strconv.ParseUint(s[digits:], base, 64)
	if err != nil || numericMagnitudeOverflowsInt64(magnitude, negative) {
		return 0, false
	}
	if !negative {
		return int64(magnitude), true
	}
	if magnitude == uint64(math.MaxInt64)+1 {
		return math.MinInt64, true
	}
	return -int64(magnitude), true
}

// startsLikeDecimalNumber reports whether s can be a decimal number: after an
// optional sign, a digit, or a '.' followed by a digit. Text that can't be one
// (e.uuid, n.name) is rejected before strconv builds a parse error for it.
func startsLikeDecimalNumber(s string) bool {
	i := 0
	if i < len(s) && (s[i] == '-' || s[i] == '+') {
		i++
	}
	if i < len(s) && s[i] == '.' {
		i++
	}
	return i < len(s) && cyphertext.IsDigit(s[i])
}

func parseFloatFast(s string) (float64, bool) {
	if !strings.ContainsAny(s, ".eE") || !startsLikeDecimalNumber(s) {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}
