package cypher

// Pipeline value helpers parse literals and coerce row values for UNWIND.

import (
	"context"
	"reflect"
	"strconv"
	"strings"

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
	return evaluateStaticListForPipelineWithCancellation(expr, row, nil)
}

func evaluateStaticListForPipelineWithCancellation(expr string, row pipelineRow, probe *pipelineCancellation) ([]interface{}, bool) {
	expr = strings.TrimSpace(expr)
	// Bare variable.
	if val, ok := row[expr]; ok {
		return coerceToUnwindItemsWithCancellation(val, probe), true
	}
	// Property access (a.b): the whole expression only, not one that starts
	// with it (UNWIND n.p + n.q is a list concatenation).
	if base, field, property := parseVarPropertyRef(expr); property {
		if baseVal, ok := row[base]; ok {
			if asMap, ok := toStringAnyMapWithCancellation(baseVal, probe); ok {
				if v, ok := asMap[field]; ok {
					return coerceToUnwindItemsWithCancellation(v, probe), true
				}
			}
			if node, ok := baseVal.(*storage.Node); ok && node != nil {
				return coerceToUnwindItemsWithCancellation(node.Properties[field], probe), true
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
		return coerceToUnwindItemsWithCancellation(parsed, probe), true
	}
	return nil, false
}

func (e *StorageExecutor) evaluateListForPipelineWithContext(ctx context.Context, expr string, row pipelineRow) ([]interface{}, bool) {
	ctx = withPipelineCancellation(ctx)
	probe := pipelineCancellationForContext(ctx)
	if inner, wrapped := stripEnclosingExpressionParentheses(strings.TrimSpace(expr)); wrapped {
		expr = inner
	}
	if mayContainSubqueryExpression(expr) {
		if value, ok := e.evaluateRowExpressionWithContext(ctx, expr, row); ok {
			return coerceToUnwindItemsWithCancellation(value, probe), true
		}
	}
	if items, ok := evaluateStaticListForPipelineWithCancellation(expr, row, probe); ok {
		return items, true
	}
	if matchFuncStartAndSuffix(expr, "range") {
		args := e.splitFunctionArgs(extractFuncArgs(expr, "range"))
		if len(args) < 2 || len(args) > 3 {
			return nil, false
		}
		arguments := make([]interface{}, len(args))
		for index, argument := range args {
			value, resolved := e.evaluateRowExpressionWithContext(ctx, strings.TrimSpace(argument), row)
			if !resolved {
				return nil, false
			}
			arguments[index] = value
		}
		budget := expressionBudgetOf(ctx)
		items, err := evaluateCypherRangeWithCancellation(&budget, arguments, probe)
		if err != nil {
			recordExpressionFailure(ctx, err)
			return nil, false
		}
		return items, true
	}
	if value, ok := e.evaluateRowExpressionWithContext(ctx, expr, row); ok {
		return coerceToUnwindItemsWithCancellation(value, probe), true
	}

	value := e.evaluateExpressionWithContext(withValueBindings(ctx, row), expr, nil, nil)
	if text, unresolved := value.(string); unresolved && text == expr && !isWholeCypherQuotedString(expr) {
		return nil, false
	}
	if value == nil && !strings.EqualFold(strings.TrimSpace(expr), "null") && !looksLikeFunctionCall(expr) {
		return nil, false
	}
	return coerceToUnwindItemsWithCancellation(value, probe), true
}

func toAnySlice(v interface{}) []interface{} {
	return toAnySliceWithCancellation(v, nil)
}

func toAnySliceWithCancellation(v interface{}, probe *pipelineCancellation) []interface{} {
	if probe != nil && !probe.check() {
		return nil
	}
	switch s := v.(type) {
	case []interface{}:
		return s
	case []map[string]interface{}:
		out := make([]interface{}, 0, pipelineInitialCapacity(len(s), probe))
		for _, m := range s {
			if probe != nil && !probe.check() {
				return nil
			}
			out = appendPipelineItem[interface{}](out, m, probe)
			if probe != nil && probe.err != nil {
				return nil
			}

		}
		return out
	case []string:
		out := make([]interface{}, 0, pipelineInitialCapacity(len(s), probe))
		for i := range s {
			if probe != nil && !probe.check() {
				return nil
			}
			out = appendPipelineItem[interface{}](out, s[i], probe)
			if probe != nil && probe.err != nil {
				return nil
			}

		}
		return out
	case []int:
		out := make([]interface{}, 0, pipelineInitialCapacity(len(s), probe))
		for i := range s {
			if probe != nil && !probe.check() {
				return nil
			}
			out = appendPipelineItem[interface{}](out, int64(s[i]), probe)
			if probe != nil && probe.err != nil {
				return nil
			}

		}
		return out
	case []int64:
		out := make([]interface{}, 0, pipelineInitialCapacity(len(s), probe))
		for i := range s {
			if probe != nil && !probe.check() {
				return nil
			}
			out = appendPipelineItem[interface{}](out, s[i], probe)
			if probe != nil && probe.err != nil {
				return nil
			}

		}
		return out
	case []float64:
		out := make([]interface{}, 0, pipelineInitialCapacity(len(s), probe))
		for i := range s {
			if probe != nil && !probe.check() {
				return nil
			}
			out = appendPipelineItem[interface{}](out, s[i], probe)
			if probe != nil && probe.err != nil {
				return nil
			}

		}
		return out
	case []float32:
		out := make([]interface{}, 0, pipelineInitialCapacity(len(s), probe))
		for i := range s {
			if probe != nil && !probe.check() {
				return nil
			}
			out = appendPipelineItem[interface{}](out, float64(s[i]), probe)
			if probe != nil && probe.err != nil {
				return nil
			}

		}
		return out
	case []bool:
		out := make([]interface{}, 0, pipelineInitialCapacity(len(s), probe))
		for i := range s {
			if probe != nil && !probe.check() {
				return nil
			}
			out = appendPipelineItem[interface{}](out, s[i], probe)
			if probe != nil && probe.err != nil {
				return nil
			}

		}
		return out
	}
	rv := reflect.ValueOf(v)
	if !rv.IsValid() || (rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array) {
		return nil
	}
	out := make([]interface{}, 0, pipelineInitialCapacity(rv.Len(), probe))
	for i := 0; i < rv.Len(); i++ {
		if probe != nil && !probe.check() {
			return nil
		}
		out = appendPipelineItem[interface{}](out, rv.Index(i).Interface(), probe)
		if probe != nil && probe.err != nil {
			return nil
		}

	}
	return out
}

func parseLiteralMapForPipeline(s string) map[string]interface{} {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "{") || !strings.HasSuffix(s, "}") {
		return nil
	}
	inner := strings.TrimSpace(s[1 : len(s)-1])
	if inner == "" {
		return map[string]interface{}{}
	}
	pairs := splitTopLevelComma(inner)
	out := make(map[string]interface{}, len(pairs))
	for _, pair := range pairs {
		colon := findTopLevelMapKeyValueSeparator(pair)
		if colon <= 0 {
			return nil
		}
		k := normalizePropertyKey(strings.TrimSpace(pair[:colon]))
		vRaw := strings.TrimSpace(pair[colon+1:])
		v, ok := parseLiteralValueForPipeline(vRaw)
		if !ok {
			return nil
		}
		out[k] = v
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
		return decodeCypherQuotedString(s)
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
	base := 10
	if digits+2 <= len(s) && s[digits] == '0' {
		switch s[digits+1] {
		case 'x', 'X':
			base = 16
			digits += 2
		case 'o', 'O':
			base = 8
			digits += 2
		}
	}
	if base == 10 {
		// Only digits can follow the sign; checking first keeps text that
		// isn't a number (n.name, e.uuid) from building a parse error.
		for i := digits; i < len(s); i++ {
			if s[i] < '0' || s[i] > '9' {
				return 0, false
			}
		}
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
	return i < len(s) && s[i] >= '0' && s[i] <= '9'
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
