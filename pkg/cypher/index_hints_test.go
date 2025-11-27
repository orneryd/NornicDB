package cypher

import (
	"testing"
)

func TestParseIndexHints(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		expectedHints int
		expectedClean string
		checkHint     func([]IndexHint) bool
	}{
		{
			name:          "single index hint",
			query:         "MATCH (n:Person) USING INDEX n:Person(name) WHERE n.name = 'Alice' RETURN n",
			expectedHints: 1,
			expectedClean: "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n",
			checkHint: func(hints []IndexHint) bool {
				return hints[0].Type == HintIndex &&
					hints[0].Variable == "n" &&
					hints[0].Label == "Person" &&
					hints[0].Property == "name"
			},
		},
		{
			name:          "multiple index hints",
			query:         "MATCH (a:Person)-[:KNOWS]->(b:Person) USING INDEX a:Person(name) USING INDEX b:Person(email) WHERE a.name = 'Alice' RETURN a, b",
			expectedHints: 2,
			expectedClean: "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = 'Alice' RETURN a, b",
			checkHint: func(hints []IndexHint) bool {
				return hints[0].Variable == "a" && hints[0].Property == "name" &&
					hints[1].Variable == "b" && hints[1].Property == "email"
			},
		},
		{
			name:          "scan hint",
			query:         "MATCH (n:Person) USING SCAN n:Person WHERE n.age > 30 RETURN n",
			expectedHints: 1,
			expectedClean: "MATCH (n:Person) WHERE n.age > 30 RETURN n",
			checkHint: func(hints []IndexHint) bool {
				return hints[0].Type == HintScan &&
					hints[0].Variable == "n" &&
					hints[0].Label == "Person"
			},
		},
		{
			name:          "join hint",
			query:         "MATCH (a:Person)-[:KNOWS]->(b:Person) USING JOIN ON a WHERE a.name = 'Alice' RETURN a, b",
			expectedHints: 1,
			expectedClean: "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = 'Alice' RETURN a, b",
			checkHint: func(hints []IndexHint) bool {
				return hints[0].Type == HintJoin &&
					hints[0].Variable == "a"
			},
		},
		{
			name:          "composite index hint",
			query:         "MATCH (n:Person) USING INDEX n:Person(firstName, lastName) WHERE n.firstName = 'John' RETURN n",
			expectedHints: 1,
			expectedClean: "MATCH (n:Person) WHERE n.firstName = 'John' RETURN n",
			checkHint: func(hints []IndexHint) bool {
				return hints[0].Type == HintIndex &&
					hints[0].Property == "firstName" &&
					len(hints[0].Properties) == 2 &&
					hints[0].Properties[1] == "lastName"
			},
		},
		{
			name:          "no hints",
			query:         "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n",
			expectedHints: 0,
			expectedClean: "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n",
			checkHint:     nil,
		},
		{
			name:          "case insensitive",
			query:         "MATCH (n:Person) using index n:Person(name) WHERE n.name = 'Alice' RETURN n",
			expectedHints: 1,
			expectedClean: "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n",
			checkHint: func(hints []IndexHint) bool {
				return hints[0].Type == HintIndex &&
					hints[0].Variable == "n" &&
					hints[0].Label == "Person"
			},
		},
		{
			name:          "mixed hints",
			query:         "MATCH (a:Person)-[:KNOWS]->(b:Person) USING INDEX a:Person(name) USING SCAN b:Person WHERE a.name = 'Alice' RETURN a, b",
			expectedHints: 2,
			expectedClean: "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = 'Alice' RETURN a, b",
			checkHint: func(hints []IndexHint) bool {
				hasIndex := false
				hasScan := false
				for _, h := range hints {
					if h.Type == HintIndex && h.Variable == "a" {
						hasIndex = true
					}
					if h.Type == HintScan && h.Variable == "b" {
						hasScan = true
					}
				}
				return hasIndex && hasScan
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hints, cleanQuery := ParseIndexHints(tt.query)

			if len(hints) != tt.expectedHints {
				t.Errorf("expected %d hints, got %d", tt.expectedHints, len(hints))
			}

			if cleanQuery != tt.expectedClean {
				t.Errorf("expected clean query:\n%s\ngot:\n%s", tt.expectedClean, cleanQuery)
			}

			if tt.checkHint != nil && !tt.checkHint(hints) {
				t.Errorf("hint check failed for hints: %+v", hints)
			}
		})
	}
}

func TestIndexHintContext(t *testing.T) {
	hints := []IndexHint{
		{Type: HintIndex, Variable: "n", Label: "Person", Property: "name"},
		{Type: HintIndex, Variable: "n", Label: "Person", Property: "email"},
		{Type: HintScan, Variable: "m", Label: "Movie"},
	}

	ctx := NewIndexHintContext(hints)

	t.Run("GetHintsForVariable", func(t *testing.T) {
		nHints := ctx.GetHintsForVariable("n")
		if len(nHints) != 2 {
			t.Errorf("expected 2 hints for 'n', got %d", len(nHints))
		}

		mHints := ctx.GetHintsForVariable("m")
		if len(mHints) != 1 {
			t.Errorf("expected 1 hint for 'm', got %d", len(mHints))
		}

		xHints := ctx.GetHintsForVariable("x")
		if len(xHints) != 0 {
			t.Errorf("expected 0 hints for 'x', got %d", len(xHints))
		}
	})

	t.Run("HasIndexHint", func(t *testing.T) {
		if !ctx.HasIndexHint("n", "Person", "name") {
			t.Error("expected index hint for n:Person(name)")
		}

		if !ctx.HasIndexHint("n", "Person", "email") {
			t.Error("expected index hint for n:Person(email)")
		}

		if ctx.HasIndexHint("n", "Person", "age") {
			t.Error("unexpected index hint for n:Person(age)")
		}

		if ctx.HasIndexHint("m", "Movie", "title") {
			t.Error("unexpected index hint for m:Movie(title)")
		}
	})

	t.Run("ShouldForceScan", func(t *testing.T) {
		if !ctx.ShouldForceScan("m", "Movie") {
			t.Error("expected scan hint for m:Movie")
		}

		if ctx.ShouldForceScan("n", "Person") {
			t.Error("unexpected scan hint for n:Person")
		}
	})
}

func TestIndexHintString(t *testing.T) {
	tests := []struct {
		hint     IndexHint
		expected string
	}{
		{
			hint:     IndexHint{Type: HintIndex, Variable: "n", Label: "Person", Property: "name"},
			expected: "USING INDEX n:Person(name)",
		},
		{
			hint:     IndexHint{Type: HintIndex, Variable: "n", Label: "Person", Property: "firstName", Properties: []string{"firstName", "lastName"}},
			expected: "USING INDEX n:Person(firstName, lastName)",
		},
		{
			hint:     IndexHint{Type: HintScan, Variable: "n", Label: "Person"},
			expected: "USING SCAN n:Person",
		},
		{
			hint:     IndexHint{Type: HintJoin, Variable: "n"},
			expected: "USING JOIN ON n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.hint.String()
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestCompareValues(t *testing.T) {
	tests := []struct {
		name     string
		a, b     interface{}
		expected bool
	}{
		{"nil == nil", nil, nil, true},
		{"nil != value", nil, "test", false},
		{"same string", "test", "test", true},
		// Note: compareValues in case_expression.go uses sprintf which is case-sensitive
		{"case sensitive", "Test", "test", false},
		{"int == int", 42, 42, true},
		{"int == float64", 42, 42.0, true},
		{"int64 == float64", int64(42), 42.0, true},
		{"different numbers", 42, 43, false},
		// Note: compareValues in case_expression.go converts numeric strings to float64
		{"string_42 == number_42", "42", 42, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareValues(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("compareValues(%v, %v) = %v, expected %v", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

func TestNilIndexHintContext(t *testing.T) {
	var ctx *IndexHintContext = nil

	// Should not panic
	hints := ctx.GetHintsForVariable("n")
	if len(hints) != 0 {
		t.Error("expected empty hints for nil context")
	}

	if ctx.HasIndexHint("n", "Person", "name") {
		t.Error("expected false for nil context")
	}

	if ctx.ShouldForceScan("n", "Person") {
		t.Error("expected false for nil context")
	}
}
