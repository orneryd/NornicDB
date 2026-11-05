package cypher

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFunctionCallRecognition(t *testing.T) {
	type recognitionAPI uint8
	const (
		prefixRecognition recognitionAPI = iota
		suffixRecognition
		standaloneRecognition
	)
	type input struct {
		name, expr, funcName string
		want                 bool
	}
	for _, group := range []struct {
		api   recognitionAPI
		name  string
		cases []input
	}{
		{api: prefixRecognition, name: "matchFuncStart", cases: []input{
			// Basic matches - no whitespace
			{"basic count", "count(n)", "count", true},
			{"basic COUNT uppercase", "COUNT(n)", "count", true},
			{"basic Count mixed case", "Count(n)", "count", true},
			{"basic sum", "sum(x)", "sum", true},

			// With whitespace before paren
			{"space before paren", "count (n)", "count", true},
			{"multiple spaces", "count   (n)", "count", true},
			{"tab before paren", "count\t(n)", "count", true},
			{"newline before paren", "count\n(n)", "count", true},
			{"mixed whitespace", "count \t\n (n)", "count", true},

			// Case insensitivity
			{"uppercase with space", "COUNT (n)", "count", true},
			{"mixed case with space", "CoUnT (n)", "count", true},

			// Should NOT match
			{"different function", "countx(n)", "count", false},
			{"prefix mismatch", "xcount(n)", "count", false},
			{"no paren", "count n", "count", false},
			{"empty string", "", "count", false},
			{"just function name", "count", "count", false},

			// Edge cases
			{"function with dot", "apoc.text.join(x)", "apoc.text.join", true},
			{"function with dot and space", "apoc.text.join (x)", "apoc.text.join", true},
			{"nested function outer", "tolower(substring(x))", "tolower", true},
			{"nested function inner should fail", "tolower(substring(x))", "substring", false},
		}},
		{api: suffixRecognition, name: "matchFuncStartAndSuffix", cases: []input{
			// Should match
			{"basic", "count(n)", "count", true},
			{"with space", "count (n)", "count", true},
			{"uppercase", "COUNT(n)", "count", true},
			{"complex args", "substring('hello', 0, 3)", "substring", true},
			{"nested parens", "count(n.items[0])", "count", true},

			// Should NOT match - doesn't end with )
			{"extra after", "count(n) + 1", "count", false},
			{"comparison after", "count(n) > 0", "count", false},
			{"string concat", "tolower(x) + ' suffix'", "tolower", false},

			// Should NOT match - wrong function
			{"wrong func", "sum(n)", "count", false},
		}},
		{api: standaloneRecognition, name: "isFunctionCallWS", cases: []input{
			// Standalone function calls - should be true
			{"basic", "count(n)", "count", true},
			{"with space", "count (n)", "count", true},
			{"uppercase", "COUNT(n)", "count", true},
			{"complex args", "date('2025-01-01')", "date", true},
			{"nested parens in args", "count(n.items[0])", "count", true},

			// Not standalone - should be false
			{"arithmetic after", "count(n) + 1", "count", false},
			{"comparison after", "count(n) > 0", "count", false},
			{"in larger expr", "date('2025-01-01') + duration('P5D')", "date", false},
			{"string concat", "tolower(x) + ' suffix'", "tolower", false},

			// Nested calls
			{"outer func yes", "tolower(substring(x))", "tolower", true},
			{"inner func no", "tolower(substring(x))", "substring", false},

			// Edge cases
			{"quoted parens", "substring('hello()', 0, 3)", "substring", true},
			{"empty args", "now()", "now", true},
		}},
	} {
		t.Run(group.name, func(t *testing.T) {
			for _, tc := range group.cases {
				t.Run(tc.name, func(t *testing.T) {
					var got bool
					switch group.api {
					case prefixRecognition:
						got = matchFuncStart(tc.expr, tc.funcName)
					case suffixRecognition:
						got = matchFuncStartAndSuffix(tc.expr, tc.funcName)
					case standaloneRecognition:
						got = inspectFunctionCall(tc.expr, namedFunctionCall, tc.funcName, nil)
					}
					assert.Equal(t, tc.want, got, "%s(%q, %q)", group.name, tc.expr, tc.funcName)
				})
			}
		})
	}
}

func TestExtractFuncArgs(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		funcName string
		want     string
	}{
		{"basic", "count(n)", "count", "n"},
		{"with space", "count (n)", "count", "n"},
		{"multiple args", "substring('hello', 0, 3)", "substring", "'hello', 0, 3"},
		{"nested", "tolower(substring(x, 0))", "tolower", "substring(x, 0)"},
		{"complex", "coalesce(n.name, 'default')", "coalesce", "n.name, 'default'"},

		// Edge cases
		{"empty args", "count()", "count", ""},
		{"whitespace args", "count( n )", "count", "n"},
		{"wrong func", "sum(n)", "count", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractFuncArgs(tt.expr, tt.funcName)
			assert.Equal(t, tt.want, got, "extractFuncArgs(%q, %q)", tt.expr, tt.funcName)
		})
	}
}

func TestCachingPerformance(t *testing.T) {
	// Verify caching works by calling same function multiple times
	// First call compiles regex, subsequent calls use cache

	// Warm up cache
	matchFuncStart("count(n)", "count")

	// Should be fast due to caching
	for i := 0; i < 10000; i++ {
		matchFuncStart("count(n)", "count")
		matchFuncStart("COUNT (n)", "count")
	}

	// Different functions should also cache
	matchFuncStart("sum(x)", "sum")
	matchFuncStart("avg(x)", "avg")
}

// Benchmark to compare old vs new approach
func BenchmarkMatchFuncStart(b *testing.B) {
	expr := "count (n)"
	funcName := "count"

	b.Run("new_matchFuncStart", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			matchFuncStart(expr, funcName)
		}
	})

	b.Run("old_HasPrefix", func(b *testing.B) {
		lowerExpr := "count (n)"
		for i := 0; i < b.N; i++ {
			_ = len(lowerExpr) > 6 && lowerExpr[:6] == "count("
		}
	})
}

// The function-match helpers on input with no call to read: a nil matcher,
// an empty name, a missing or unclosed parenthesis.
func TestFunctionMatchHelpersWithoutCall(t *testing.T) {
	var nilMatcher *funcMatcher
	assert.False(t, nilMatcher.MatchString("count(n)"))
	assert.Empty(t, nilMatcher.String())
	assert.Equal(t, "func:", getFuncMatcher("  ").String())
	assert.Empty(t, extractFuncArgs("count(n", "count"))
	args, open := extractFuncArgsLen("count(n", "count")
	assert.Empty(t, args)
	assert.Equal(t, -1, open)
	_, _, ok := extractFuncArgsWithSuffix("count(n", "count")
	assert.False(t, ok)
	_, open = extractFuncArgsLen("sum(n)", "count")
	assert.Equal(t, -1, open)
	_, _, ok = extractFuncArgsWithSuffix("sum(n)", "count")
	assert.False(t, ok)
}
