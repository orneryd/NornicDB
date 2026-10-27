package cypher

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// randomWhitespace generates random whitespace (space, tab, newline, or combination)
func randomWhitespace(rng *rand.Rand) string {
	wsChars := []string{" ", "\t", "\n", "  ", "\t ", " \t", " \n", "\n "}
	count := rng.Intn(3) + 1 // 1-3 whitespace segments
	var ws strings.Builder
	for i := 0; i < count; i++ {
		ws.WriteString(wsChars[rng.Intn(len(wsChars))])
	}
	return ws.String()
}

// randomCase randomly changes case of each character
func randomCase(rng *rand.Rand, s string) string {
	var result strings.Builder
	for _, c := range s {
		if rng.Intn(2) == 0 {
			result.WriteString(strings.ToUpper(string(c)))
		} else {
			result.WriteString(strings.ToLower(string(c)))
		}
	}
	return result.String()
}

// TestMatchFuncStartChaos tests function matching with randomized whitespace
func TestMatchFuncStartChaos(t *testing.T) {
	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(seed))
	t.Logf("Chaos test seed: %d (use this to reproduce failures)", seed)

	// Functions to test - covering different categories
	functions := []string{
		// Simple functions
		"count", "sum", "avg", "min", "max", "collect",
		// String functions
		"tolower", "toupper", "trim", "substring", "replace", "split",
		// Type conversion
		"tostring", "tointeger", "tofloat", "toboolean",
		// List functions
		"head", "last", "tail", "reverse", "range", "slice",
		// Node/relationship functions
		"id", "elementid", "labels", "type", "keys", "properties",
		// Spatial functions
		"point", "distance", "withinbbox",
		// Functions with dots
		"apoc.map.merge", "apoc.text.join", "point.x", "point.latitude",
		"kalman.init", "vector.similarity.cosine",
	}

	for _, funcName := range functions {
		t.Run(fmt.Sprintf("func_%s", funcName), func(t *testing.T) {
			// Run multiple iterations with different random whitespace
			for i := 0; i < 10; i++ {
				// Generate random whitespace before paren
				ws := randomWhitespace(rng)
				// Generate random case
				randomFuncName := randomCase(rng, funcName)

				// Create expression with whitespace: "funcName WS (args)"
				expr := fmt.Sprintf("%s%s(x)", randomFuncName, ws)

				// Should match
				matched := matchFuncStart(expr, funcName)
				assert.True(t, matched, "matchFuncStart should match '%s' (ws=%q)", expr, ws)

				// matchFuncStartAndSuffix should also match for complete expressions
				exprComplete := fmt.Sprintf("%s%s(x)", randomFuncName, ws)
				matchedComplete := matchFuncStartAndSuffix(exprComplete, funcName)
				assert.True(t, matchedComplete, "matchFuncStartAndSuffix should match '%s'", exprComplete)

				// extractFuncArgs should work
				args := extractFuncArgs(exprComplete, funcName)
				assert.Equal(t, "x", args, "extractFuncArgs should extract 'x' from '%s'", exprComplete)
			}
		})
	}
}

// TestMatchFuncStartNegativeChaos ensures we don't false-positive match similar names
func TestMatchFuncStartNegativeChaos(t *testing.T) {
	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(seed))
	t.Logf("Chaos test seed: %d", seed)

	// Test cases where we should NOT match
	testCases := []struct {
		expr     string
		funcName string
		reason   string
	}{
		{"counter(x)", "count", "different function with count prefix"},
		{"counting(x)", "count", "different function with count prefix"},
		{"xcount(x)", "count", "prefix before function name"},
		{"my_count(x)", "count", "underscore prefix"},
		{"sum_total(x)", "sum", "different function with sum prefix"},
		{"summary(x)", "sum", "different function with sum prefix"},
		{"average(x)", "avg", "different function"},
		{"tostringify(x)", "tostring", "different function with tostring prefix"},
		{"pointer(x)", "point", "different function with point prefix"},
		{"distance_km(x)", "distance", "different function with distance prefix"},
		{"apoc.map.mergeAll(x)", "apoc.map.merge", "different function with same prefix"},
	}

	for _, tc := range testCases {
		t.Run(tc.reason, func(t *testing.T) {
			// Add random case to expr
			expr := randomCase(rng, tc.expr)
			matched := matchFuncStart(expr, tc.funcName)
			assert.False(t, matched, "should NOT match '%s' for func '%s': %s", expr, tc.funcName, tc.reason)
		})
	}
}

// TestIsFunctionCallWSChaos tests standalone function detection with chaos whitespace
func TestIsFunctionCallWSChaos(t *testing.T) {
	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(seed))
	t.Logf("Chaos test seed: %d", seed)

	// Standalone function calls - should return true
	standaloneCases := []struct {
		template string // Use %s for whitespace placeholder
		funcName string
	}{
		{"count%s(n)", "count"},
		{"COUNT%s(n)", "count"},
		{"sum%s(x.value)", "sum"},
		{"toLower%s(name)", "tolower"},
		{"SUBSTRING%s('hello', 0, 3)", "substring"},
		{"point%s({x: 1, y: 2})", "point"},
		{"apoc.map.merge%s(m1, m2)", "apoc.map.merge"},
	}

	for _, tc := range standaloneCases {
		t.Run(fmt.Sprintf("standalone_%s", tc.funcName), func(t *testing.T) {
			for i := 0; i < 5; i++ {
				ws := randomWhitespace(rng)
				expr := fmt.Sprintf(tc.template, ws)
				result := inspectFunctionCall(expr, namedFunctionCall, tc.funcName, nil)
				assert.True(t, result, "isFunctionCallWS should return true for standalone '%s'", expr)
			}
		})
	}

	// Non-standalone expressions - should return false
	nonStandaloneCases := []struct {
		template string
		funcName string
		reason   string
	}{
		{"count%s(n) + 1", "count", "arithmetic after"},
		{"count%s(n) > 0", "count", "comparison after"},
		{"sum%s(x) * 2", "sum", "multiplication after"},
		{"toLower%s(x) + ' suffix'", "tolower", "string concat after"},
		{"point%s({x:1}) + offset", "point", "addition after"},
	}

	for _, tc := range nonStandaloneCases {
		t.Run(fmt.Sprintf("non_standalone_%s_%s", tc.funcName, tc.reason), func(t *testing.T) {
			for i := 0; i < 3; i++ {
				ws := randomWhitespace(rng)
				expr := fmt.Sprintf(tc.template, ws)
				result := inspectFunctionCall(expr, namedFunctionCall, tc.funcName, nil)
				assert.False(t, result, "isFunctionCallWS should return false for non-standalone '%s'", expr)
			}
		})
	}
}

// TestChaosNestedFunctions tests nested function calls with random whitespace
func TestChaosNestedFunctions(t *testing.T) {
	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(seed))
	t.Logf("Chaos test seed: %d", seed)

	for i := 0; i < 10; i++ {
		// Generate random whitespace for different positions
		ws1 := randomWhitespace(rng)
		ws2 := randomWhitespace(rng)

		// Nested: tolower(substring(x, 0))
		nested := fmt.Sprintf("toLower%s(substring%s(x, 0))", ws1, ws2)

		// Outer function should match as standalone
		assert.True(t, inspectFunctionCall(nested, namedFunctionCall, "tolower", nil),
			"outer function should be standalone in '%s'", nested)

		// Inner function should NOT match as standalone (it's nested)
		assert.False(t, inspectFunctionCall(nested, namedFunctionCall, "substring", nil),
			"inner function should NOT be standalone in '%s'", nested)

		// But inner function should match with matchFuncStart when extracted
		innerStart := strings.Index(strings.ToLower(nested), "substring")
		if innerStart > 0 {
			innerExpr := nested[innerStart:]
			// This starts with substring but has extra ) at the end
			assert.True(t, matchFuncStart(innerExpr, "substring"),
				"inner should match start in '%s'", innerExpr)
		}
	}
}

// TestChaosComplexArguments tests function argument extraction with complex content
func TestChaosComplexArguments(t *testing.T) {
	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(seed))
	t.Logf("Chaos test seed: %d", seed)

	testCases := []struct {
		name         string
		exprTemplate string // %s for whitespace before paren
		funcName     string
		expectedArgs string
	}{
		{
			"simple_arg",
			"count%s(n)",
			"count",
			"n",
		},
		{
			"string_with_parens",
			"substring%s('hello(world)', 0)",
			"substring",
			"'hello(world)', 0",
		},
		{
			"nested_call",
			"tolower%s(substring(x, 0, 5))",
			"tolower",
			"substring(x, 0, 5)",
		},
		{
			"map_literal",
			"point%s({x: 10, y: 20})",
			"point",
			"{x: 10, y: 20}",
		},
		{
			"array_literal",
			"size%s([1, 2, 3])",
			"size",
			"[1, 2, 3]",
		},
		{
			"multiple_args",
			"coalesce%s(n.name, n.title, 'default')",
			"coalesce",
			"n.name, n.title, 'default'",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i < 5; i++ {
				ws := randomWhitespace(rng)
				expr := fmt.Sprintf(tc.exprTemplate, ws)

				// Should match
				require.True(t, matchFuncStartAndSuffix(expr, tc.funcName),
					"should match '%s'", expr)

				// Should extract correct args
				args := extractFuncArgs(expr, tc.funcName)
				assert.Equal(t, tc.expectedArgs, args,
					"extractFuncArgs('%s', '%s') should return '%s'", expr, tc.funcName, tc.expectedArgs)
			}
		})
	}
}

// TestChaosEdgeCases tests edge cases with various whitespace patterns
func TestChaosEdgeCases(t *testing.T) {
	testCases := []struct {
		name     string
		expr     string
		funcName string
		match    bool
	}{
		// Tab before paren
		{"tab_before_paren", "count\t(n)", "count", true},
		// Newline before paren
		{"newline_before_paren", "count\n(n)", "count", true},
		// Multiple newlines
		{"multiple_newlines", "count\n\n(n)", "count", true},
		// Tab and space mix
		{"tab_space_mix", "count \t (n)", "count", true},
		// Carriage return + newline (Windows)
		{"crlf_before_paren", "count\r\n(n)", "count", true},
		// Just function name (no paren)
		{"no_paren", "count", "count", false},
		// Space after paren (doesn't affect matching)
		{"space_after_paren", "count( n )", "count", true},
		// Empty args
		{"empty_args", "now ()", "now", true},
		// Unicode whitespace uses the same accepted whitespace owner as statements.
		{"non_breaking_space", "count\u00A0(n)", "count", true}, // NBSP
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := matchFuncStartAndSuffix(tc.expr, tc.funcName)
			if tc.match {
				assert.True(t, result, "should match: %s", tc.name)
			} else {
				assert.False(t, result, "should NOT match: %s", tc.name)
			}
		})
	}
}

// TestChaosQueryPatterns tests realistic query patterns with random formatting
func TestChaosQueryPatterns(t *testing.T) {
	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(seed))
	t.Logf("Chaos test seed: %d", seed)

	// Simulate expressions that might appear in real Cypher queries
	patterns := []struct {
		name     string
		generate func(rng *rand.Rand) string
		funcName string
	}{
		{
			"count_star",
			func(rng *rand.Rand) string {
				ws := randomWhitespace(rng)
				return fmt.Sprintf("COUNT%s(*)", ws)
			},
			"count",
		},
		{
			"sum_property",
			func(rng *rand.Rand) string {
				ws := randomWhitespace(rng)
				return fmt.Sprintf("SUM%s(n.amount)", ws)
			},
			"sum",
		},
		{
			"collect_distinct",
			func(rng *rand.Rand) string {
				ws := randomWhitespace(rng)
				return fmt.Sprintf("COLLECT%s(DISTINCT n.category)", ws)
			},
			"collect",
		},
		{
			"tolower_property",
			func(rng *rand.Rand) string {
				ws := randomWhitespace(rng)
				return fmt.Sprintf("toLower%s(n.name)", ws)
			},
			"tolower",
		},
		{
			"substring_with_params",
			func(rng *rand.Rand) string {
				ws := randomWhitespace(rng)
				return fmt.Sprintf("SUBSTRING%s(n.title, 0, 100)", ws)
			},
			"substring",
		},
		{
			"coalesce_multiple",
			func(rng *rand.Rand) string {
				ws := randomWhitespace(rng)
				return fmt.Sprintf("coalesce%s(n.nickname, n.name, 'Anonymous')", ws)
			},
			"coalesce",
		},
		{
			"apoc_function",
			func(rng *rand.Rand) string {
				ws := randomWhitespace(rng)
				return fmt.Sprintf("apoc.map.merge%s(props1, props2)", ws)
			},
			"apoc.map.merge",
		},
		{
			"spatial_distance",
			func(rng *rand.Rand) string {
				ws := randomWhitespace(rng)
				return fmt.Sprintf("distance%s(point1, point2)", ws)
			},
			"distance",
		},
	}

	for _, p := range patterns {
		t.Run(p.name, func(t *testing.T) {
			for i := 0; i < 10; i++ {
				expr := p.generate(rng)
				assert.True(t, matchFuncStartAndSuffix(expr, p.funcName),
					"should match generated expr '%s'", expr)

				// Also test isFunctionCallWS for standalone
				assert.True(t, inspectFunctionCall(expr, namedFunctionCall, p.funcName, nil),
					"should be standalone function call '%s'", expr)
			}
		})
	}
}

// TestChaosNoFalsePositiveInExpressions ensures functions in larger expressions
// are correctly identified or rejected based on context
func TestChaosNoFalsePositiveInExpressions(t *testing.T) {
	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(seed))
	t.Logf("Chaos test seed: %d", seed)

	// Expressions where the function IS at the start but NOT standalone
	expressionsStartingWithFunc := []struct {
		template string // %s for whitespace
		funcName string
	}{
		// Arithmetic operations after
		{"count%s(n) + count(m)", "count"},
		{"sum%s(x) - sum(y)", "sum"},
		{"avg%s(scores) / 100", "avg"},

		// Comparisons
		{"count%s(n) > 10", "count"},
		{"length%s(str) <= maxLen", "length"},
		{"size%s(list) = 0", "size"},

		// Boolean operations
		{"exists%s(n.prop) AND n.active", "exists"},
		{"count%s(n) > 0 OR fallback", "count"},

		// String concatenation (function at start)
		{"tolower%s(name) + '@domain.com'", "tolower"},
		{"tostring%s(id) + '_suffix'", "tostring"},

		// Property access after
		{"point%s({x:1, y:2}).x", "point"},
	}

	for _, e := range expressionsStartingWithFunc {
		t.Run(fmt.Sprintf("not_standalone_%s", e.funcName), func(t *testing.T) {
			for i := 0; i < 5; i++ {
				ws := randomWhitespace(rng)
				expr := fmt.Sprintf(e.template, ws)

				// matchFuncStart should still match (it only checks the start)
				assert.True(t, matchFuncStart(expr, e.funcName),
					"matchFuncStart should match start of '%s'", expr)

				// But isFunctionCallWS should NOT match (not standalone)
				assert.False(t, inspectFunctionCall(expr, namedFunctionCall, e.funcName, nil),
					"isFunctionCallWS should NOT match '%s' (not standalone)", expr)
			}
		})
	}

	// Expressions where the function is NOT at the start (embedded in larger expression)
	expressionsWithFuncNotAtStart := []struct {
		template string // %s for whitespace
		funcName string
	}{
		// Function not at start
		{"'prefix_' + tostring%s(id)", "tostring"},
		{"1 + count%s(n)", "count"},
		{"x * sum%s(y)", "sum"},
	}

	for _, e := range expressionsWithFuncNotAtStart {
		t.Run(fmt.Sprintf("func_not_at_start_%s", e.funcName), func(t *testing.T) {
			for i := 0; i < 3; i++ {
				ws := randomWhitespace(rng)
				expr := fmt.Sprintf(e.template, ws)

				// matchFuncStart should NOT match (function is not at start)
				assert.False(t, matchFuncStart(expr, e.funcName),
					"matchFuncStart should NOT match '%s' (func not at start)", expr)

				// isFunctionCallWS should also NOT match
				assert.False(t, inspectFunctionCall(expr, namedFunctionCall, e.funcName, nil),
					"isFunctionCallWS should NOT match '%s' (func not at start)", expr)
			}
		})
	}
}

// BenchmarkChaosFunctionMatching benchmarks the regex-based matching
func BenchmarkChaosFunctionMatching(b *testing.B) {
	rng := rand.New(rand.NewSource(42)) // Fixed seed for reproducibility
	expressions := make([]string, 100)

	// Pre-generate expressions with random whitespace
	for i := 0; i < 100; i++ {
		ws := randomWhitespace(rng)
		expressions[i] = fmt.Sprintf("count%s(n.items)", ws)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		expr := expressions[i%100]
		matchFuncStartAndSuffix(expr, "count")
	}
}
