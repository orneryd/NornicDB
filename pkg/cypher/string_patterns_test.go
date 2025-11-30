package cypher

import (
	"testing"
)

func TestSplitByKeyword(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		keyword  string
		expected []string
	}{
		{
			name:     "single MATCH",
			input:    "MATCH (n) RETURN n",
			keyword:  "MATCH",
			expected: []string{"", "(n) RETURN n"},
		},
		{
			name:     "multiple MATCH",
			input:    "MATCH (a) MATCH (b) RETURN a, b",
			keyword:  "MATCH",
			expected: []string{"", "(a) ", "(b) RETURN a, b"},
		},
		{
			name:     "lowercase match",
			input:    "match (n) return n",
			keyword:  "MATCH",
			expected: []string{"", "(n) return n"},
		},
		{
			name:     "no keyword",
			input:    "CREATE (n) RETURN n",
			keyword:  "MATCH",
			expected: []string{"CREATE (n) RETURN n"},
		},
		{
			name:     "empty string",
			input:    "",
			keyword:  "MATCH",
			expected: []string{""},
		},
		{
			name:     "keyword at end without space",
			input:    "test MATCH",
			keyword:  "MATCH",
			expected: []string{"test MATCH"},
		},
		{
			name:     "CREATE split",
			input:    "CREATE (a) CREATE (b)",
			keyword:  "CREATE",
			expected: []string{"", "(a) ", "(b)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SplitByKeyword(tt.input, tt.keyword)
			if len(result) != len(tt.expected) {
				t.Errorf("SplitByKeyword(%q, %q) = %v (len=%d), want %v (len=%d)",
					tt.input, tt.keyword, result, len(result), tt.expected, len(tt.expected))
				return
			}
			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("SplitByKeyword(%q, %q)[%d] = %q, want %q",
						tt.input, tt.keyword, i, result[i], tt.expected[i])
				}
			}
		})
	}
}

func TestSplitByMatch(t *testing.T) {
	result := SplitByMatch("MATCH (a) MATCH (b)")
	if len(result) != 3 {
		t.Errorf("SplitByMatch returned %d parts, want 3", len(result))
	}
}

func TestSplitByCreate(t *testing.T) {
	result := SplitByCreate("CREATE (a) CREATE (b)")
	if len(result) != 3 {
		t.Errorf("SplitByCreate returned %d parts, want 3", len(result))
	}
}

func TestExtractLimit(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int
		found    bool
	}{
		{
			name:     "simple limit",
			input:    "MATCH (n) RETURN n LIMIT 10",
			expected: 10,
			found:    true,
		},
		{
			name:     "limit with skip",
			input:    "MATCH (n) RETURN n SKIP 5 LIMIT 20",
			expected: 20,
			found:    true,
		},
		{
			name:     "lowercase limit",
			input:    "MATCH (n) RETURN n limit 15",
			expected: 15,
			found:    true,
		},
		{
			name:     "no limit",
			input:    "MATCH (n) RETURN n",
			expected: 0,
			found:    false,
		},
		{
			name:     "limit with extra spaces",
			input:    "MATCH (n) RETURN n LIMIT   25",
			expected: 25,
			found:    true,
		},
		{
			name:     "large limit",
			input:    "MATCH (n) RETURN n LIMIT 1000000",
			expected: 1000000,
			found:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, found := ExtractLimit(tt.input)
			if found != tt.found {
				t.Errorf("ExtractLimit(%q) found=%v, want found=%v", tt.input, found, tt.found)
			}
			if result != tt.expected {
				t.Errorf("ExtractLimit(%q) = %d, want %d", tt.input, result, tt.expected)
			}
		})
	}
}

func TestExtractSkip(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int
		found    bool
	}{
		{
			name:     "simple skip",
			input:    "MATCH (n) RETURN n SKIP 5",
			expected: 5,
			found:    true,
		},
		{
			name:     "skip with limit",
			input:    "MATCH (n) RETURN n SKIP 10 LIMIT 20",
			expected: 10,
			found:    true,
		},
		{
			name:     "no skip",
			input:    "MATCH (n) RETURN n LIMIT 10",
			expected: 0,
			found:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, found := ExtractSkip(tt.input)
			if found != tt.found {
				t.Errorf("ExtractSkip(%q) found=%v, want found=%v", tt.input, found, tt.found)
			}
			if result != tt.expected {
				t.Errorf("ExtractSkip(%q) = %d, want %d", tt.input, result, tt.expected)
			}
		})
	}
}

func TestExtractLimitString(t *testing.T) {
	result := ExtractLimitString("MATCH (n) RETURN n LIMIT 42")
	if result != "42" {
		t.Errorf("ExtractLimitString returned %q, want %q", result, "42")
	}

	result = ExtractLimitString("MATCH (n) RETURN n")
	if result != "" {
		t.Errorf("ExtractLimitString returned %q, want empty string", result)
	}
}

func TestFindKeywordIndexStringPatterns(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		keyword  string
		expected int
	}{
		{
			name:     "find MATCH",
			input:    "MATCH (n) RETURN n",
			keyword:  "MATCH",
			expected: 0,
		},
		{
			name:     "find CREATE mid-string",
			input:    "MATCH (n) CREATE (m)",
			keyword:  "CREATE",
			expected: 10,
		},
		{
			name:     "case insensitive",
			input:    "match (n) return n",
			keyword:  "MATCH",
			expected: 0,
		},
		{
			name:     "not found",
			input:    "MATCH (n) RETURN n",
			keyword:  "DELETE",
			expected: -1,
		},
		{
			name:     "word boundary - not partial match",
			input:    "CREATEMATCH (n)",
			keyword:  "MATCH",
			expected: -1, // MATCH is part of CREATEMATCH, not a word
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FindKeywordIndex(tt.input, tt.keyword)
			if result != tt.expected {
				t.Errorf("FindKeywordIndex(%q, %q) = %d, want %d",
					tt.input, tt.keyword, result, tt.expected)
			}
		})
	}
}

func TestContainsKeyword(t *testing.T) {
	if !ContainsKeyword("MATCH (n) RETURN n", "MATCH") {
		t.Error("ContainsKeyword should find MATCH")
	}
	if ContainsKeyword("CREATE (n)", "MATCH") {
		t.Error("ContainsKeyword should not find MATCH in CREATE query")
	}
}

// =============================================================================
// Phase 2: Aggregation and Parameter Tests
// =============================================================================

func TestParseAggregation(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *AggregationResult
	}{
		{
			name:  "COUNT with property",
			input: "COUNT(n.age)",
			expected: &AggregationResult{
				Function: "COUNT",
				Variable: "n",
				Property: "age",
			},
		},
		{
			name:  "SUM with property",
			input: "SUM(x.value)",
			expected: &AggregationResult{
				Function: "SUM",
				Variable: "x",
				Property: "value",
			},
		},
		{
			name:  "COUNT star",
			input: "COUNT(*)",
			expected: &AggregationResult{
				Function: "COUNT",
				IsStar:   true,
			},
		},
		{
			name:  "COUNT variable only",
			input: "COUNT(n)",
			expected: &AggregationResult{
				Function: "COUNT",
				Variable: "n",
			},
		},
		{
			name:  "AVG with distinct",
			input: "AVG(DISTINCT n.price)",
			expected: &AggregationResult{
				Function: "AVG",
				Variable: "n",
				Property: "price",
				Distinct: true,
			},
		},
		{
			name:  "COLLECT with property",
			input: "COLLECT(p.name)",
			expected: &AggregationResult{
				Function: "COLLECT",
				Variable: "p",
				Property: "name",
			},
		},
		{
			name:     "not an aggregation",
			input:    "n.name",
			expected: nil,
		},
		{
			name:     "empty string",
			input:    "",
			expected: nil,
		},
		{
			name:  "lowercase function",
			input: "sum(n.value)",
			expected: &AggregationResult{
				Function: "SUM",
				Variable: "n",
				Property: "value",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseAggregation(tt.input)
			if tt.expected == nil {
				if result != nil {
					t.Errorf("ParseAggregation(%q) = %+v, want nil", tt.input, result)
				}
				return
			}
			if result == nil {
				t.Errorf("ParseAggregation(%q) = nil, want %+v", tt.input, tt.expected)
				return
			}
			if result.Function != tt.expected.Function {
				t.Errorf("Function = %q, want %q", result.Function, tt.expected.Function)
			}
			if result.Variable != tt.expected.Variable {
				t.Errorf("Variable = %q, want %q", result.Variable, tt.expected.Variable)
			}
			if result.Property != tt.expected.Property {
				t.Errorf("Property = %q, want %q", result.Property, tt.expected.Property)
			}
			if result.Distinct != tt.expected.Distinct {
				t.Errorf("Distinct = %v, want %v", result.Distinct, tt.expected.Distinct)
			}
			if result.IsStar != tt.expected.IsStar {
				t.Errorf("IsStar = %v, want %v", result.IsStar, tt.expected.IsStar)
			}
		})
	}
}

func TestParseAggregationProperty(t *testing.T) {
	v, p := ParseAggregationProperty("SUM(n.price)")
	if v != "n" || p != "price" {
		t.Errorf("got (%q, %q), want (n, price)", v, p)
	}

	v, p = ParseAggregationProperty("not_agg")
	if v != "" || p != "" {
		t.Errorf("got (%q, %q), want empty strings", v, p)
	}
}

func TestExtractParameters(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "single parameter",
			input:    "WHERE n.name = $name",
			expected: []string{"name"},
		},
		{
			name:     "multiple parameters",
			input:    "WHERE n.name = $name AND n.age > $minAge",
			expected: []string{"name", "minAge"},
		},
		{
			name:     "no parameters",
			input:    "MATCH (n) RETURN n",
			expected: nil,
		},
		{
			name:     "parameter with underscore",
			input:    "WHERE n.id = $user_id",
			expected: []string{"user_id"},
		},
		{
			name:     "parameter with numbers",
			input:    "WHERE n.id = $id123",
			expected: []string{"id123"},
		},
		{
			name:     "invalid parameter (number first)",
			input:    "WHERE n.id = $123abc",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractParameters(tt.input)
			if len(result) != len(tt.expected) {
				t.Errorf("ExtractParameters(%q) = %v, want %v", tt.input, result, tt.expected)
				return
			}
			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("result[%d] = %q, want %q", i, result[i], tt.expected[i])
				}
			}
		})
	}
}

func TestReplaceParameters(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "single replacement",
			input:    "WHERE n.name = $name",
			expected: "WHERE n.name = 'Alice'",
		},
		{
			name:     "multiple replacements",
			input:    "WHERE n.name = $name AND n.age = $age",
			expected: "WHERE n.name = 'Alice' AND n.age = '30'",
		},
		{
			name:     "no parameters",
			input:    "MATCH (n) RETURN n",
			expected: "MATCH (n) RETURN n",
		},
		{
			name:     "unknown parameter kept",
			input:    "WHERE n.x = $unknown",
			expected: "WHERE n.x = $unknown",
		},
	}

	replacer := func(param string) string {
		switch param {
		case "name":
			return "'Alice'"
		case "age":
			return "'30'"
		default:
			return "$" + param
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ReplaceParameters(tt.input, replacer)
			if result != tt.expected {
				t.Errorf("ReplaceParameters(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// =============================================================================
// Benchmarks to verify performance improvements
// =============================================================================

func BenchmarkSplitByKeyword(b *testing.B) {
	query := "MATCH (a:Person) MATCH (b:Person) WHERE a.name = 'Alice' RETURN a, b"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SplitByKeyword(query, "MATCH")
	}
}

func BenchmarkSplitByKeywordRegex(b *testing.B) {
	query := "MATCH (a:Person) MATCH (b:Person) WHERE a.name = 'Alice' RETURN a, b"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matchKeywordPattern.Split(query, -1)
	}
}

func BenchmarkExtractLimit(b *testing.B) {
	query := "MATCH (n:Person) WHERE n.age > 30 RETURN n LIMIT 100"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ExtractLimit(query)
	}
}

func BenchmarkExtractLimitRegex(b *testing.B) {
	query := "MATCH (n:Person) WHERE n.age > 30 RETURN n LIMIT 100"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		limitPattern.FindStringSubmatch(query)
	}
}

// Phase 2 Benchmarks

func BenchmarkParseAggregation(b *testing.B) {
	expr := "SUM(n.price)"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ParseAggregation(expr)
	}
}

func BenchmarkParseAggregationRegex(b *testing.B) {
	expr := "SUM(n.price)"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sumPropPattern.FindStringSubmatch(expr)
	}
}

func BenchmarkReplaceParameters(b *testing.B) {
	query := "MATCH (n:Person) WHERE n.name = $name AND n.age > $minAge RETURN n"
	replacer := func(param string) string {
		return "'" + param + "_value'"
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ReplaceParameters(query, replacer)
	}
}

func BenchmarkReplaceParametersRegex(b *testing.B) {
	query := "MATCH (n:Person) WHERE n.name = $name AND n.age > $minAge RETURN n"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parameterPattern.ReplaceAllStringFunc(query, func(match string) string {
			return "'" + match[1:] + "_value'"
		})
	}
}
