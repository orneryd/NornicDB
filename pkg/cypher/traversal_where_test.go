package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFindMatchingParen tests the paren-matching function that respects quotes
func TestFindMatchingParen(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		startIdx int
		expected int
	}{
		{
			name:     "simple parens",
			input:    "(abc)",
			startIdx: 0,
			expected: 4,
		},
		{
			name:     "nested parens",
			input:    "(a(b)c)",
			startIdx: 0,
			expected: 6,
		},
		{
			name:     "parens in single quotes",
			input:    "(name: 'value (test)')",
			startIdx: 0,
			expected: 21,
		},
		{
			name:     "parens in double quotes",
			input:    `(name: "value (test)")`,
			startIdx: 0,
			expected: 21,
		},
		{
			name:     "complex - Informal Register (tú)",
			input:    "(i:IssueType {name: 'Informal Register (tú)'})",
			startIdx: 0,
			expected: 46, // UTF-8: ú is 2 bytes
		},
		{
			name:     "multiple nested",
			input:    "((a)(b))",
			startIdx: 0,
			expected: 7,
		},
		{
			name:     "no closing paren",
			input:    "(abc",
			startIdx: 0,
			expected: -1,
		},
		{
			name:     "not a paren",
			input:    "abc)",
			startIdx: 0,
			expected: -1,
		},
		{
			name:     "escaped quote in string",
			input:    `(name: 'it\'s (complex)')`,
			startIdx: 0,
			expected: 24,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := findMatchingParen(tt.input, tt.startIdx)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestParseTraversalPatternStateMachine tests parsing with special characters
func TestParseTraversalPatternStateMachine(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	tests := []struct {
		name         string
		pattern      string
		expectNil    bool
		startVar     string
		startLabels  []string
		endVar       string
		endLabels    []string
		endPropKey   string
		endPropValue interface{}
	}{
		{
			name:        "simple pattern",
			pattern:     "(a:Person)-[:KNOWS]->(b:Person)",
			expectNil:   false,
			startVar:    "a",
			startLabels: []string{"Person"},
			endVar:      "b",
			endLabels:   []string{"Person"},
		},
		{
			name:         "pattern with simple property",
			pattern:      "(e:Entry)-[:HAS_ISSUE]->(i:IssueType {name: 'Other Issue'})",
			expectNil:    false,
			startVar:     "e",
			startLabels:  []string{"Entry"},
			endVar:       "i",
			endLabels:    []string{"IssueType"},
			endPropKey:   "name",
			endPropValue: "Other Issue",
		},
		{
			name:         "pattern with parentheses in property value",
			pattern:      "(e:Entry)-[:HAS_ISSUE]->(i:IssueType {name: 'Informal Register (tú)'})",
			expectNil:    false,
			startVar:     "e",
			startLabels:  []string{"Entry"},
			endVar:       "i",
			endLabels:    []string{"IssueType"},
			endPropKey:   "name",
			endPropValue: "Informal Register (tú)",
		},
		{
			name:         "pattern with ampersand",
			pattern:      "(e:Entry)-[:IN_CATEGORY]->(c:Category {name: 'Cart & Checkout'})",
			expectNil:    false,
			startVar:     "e",
			startLabels:  []string{"Entry"},
			endVar:       "c",
			endLabels:    []string{"Category"},
			endPropKey:   "name",
			endPropValue: "Cart & Checkout",
		},
		{
			name:      "invalid pattern - no relationship",
			pattern:   "(a:Person)(b:Person)",
			expectNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.parseTraversalPatternStateMachine(tt.pattern)

			if tt.expectNil {
				assert.Nil(t, result)
				return
			}

			require.NotNil(t, result)
			assert.Equal(t, tt.startVar, result.StartNode.variable)
			assert.Equal(t, tt.startLabels, result.StartNode.labels)
			assert.Equal(t, tt.endVar, result.EndNode.variable)
			assert.Equal(t, tt.endLabels, result.EndNode.labels)

			if tt.endPropKey != "" {
				val, exists := result.EndNode.properties[tt.endPropKey]
				assert.True(t, exists, "Expected property %s to exist", tt.endPropKey)
				assert.Equal(t, tt.endPropValue, val)
			}
		})
	}
}

// TestEvaluatePathValue tests literal value parsing
func TestEvaluatePathValue(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	tests := []struct {
		name     string
		input    string
		expected interface{}
	}{
		{
			name:     "single quoted string",
			input:    "'hello world'",
			expected: "hello world",
		},
		{
			name:     "double quoted string",
			input:    `"hello world"`,
			expected: "hello world",
		},
		{
			name:     "integer",
			input:    "42",
			expected: int64(42),
		},
		{
			name:     "negative integer",
			input:    "-10",
			expected: int64(-10),
		},
		{
			name:     "float",
			input:    "3.14",
			expected: 3.14,
		},
		{
			name:     "true",
			input:    "true",
			expected: true,
		},
		{
			name:     "false",
			input:    "FALSE",
			expected: false,
		},
		{
			name:     "string with special chars",
			input:    "'Informal Register (tú)'",
			expected: "Informal Register (tú)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.evaluatePathValue(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestCompareValuesForPath tests value comparison with different operators
func TestCompareValuesForPath(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	tests := []struct {
		name     string
		left     interface{}
		right    interface{}
		op       string
		expected bool
	}{
		// String comparisons
		{"string equal", "hello", "hello", "=", true},
		{"string not equal", "hello", "world", "=", false},
		{"string not equal op", "hello", "world", "<>", true},
		{"string less than", "apple", "banana", "<", true},
		{"string greater than", "zebra", "apple", ">", true},

		// Integer comparisons
		{"int equal", int64(42), int64(42), "=", true},
		{"int not equal", int64(42), int64(43), "=", false},
		{"int less than", int64(10), int64(20), "<", true},
		{"int greater than", int64(30), int64(20), ">", true},
		{"int less or equal", int64(10), int64(10), "<=", true},
		{"int greater or equal", int64(20), int64(20), ">=", true},

		// Float comparisons
		{"float equal", 3.14, 3.14, "=", true},
		{"float less than", 2.5, 3.5, "<", true},

		// Mixed int/float
		{"int vs float", int64(42), 42.0, "=", true},

		// Nil comparisons
		{"nil equal nil", nil, nil, "=", true},
		{"nil not equal value", nil, "hello", "=", false},
		{"nil not equal op", nil, "hello", "<>", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.compareValues(tt.left, tt.right, tt.op)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestEvaluateWhereOnPath tests WHERE evaluation on path contexts
func TestEvaluateWhereOnPath(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	// Create path context with test nodes
	startNode := &storage.Node{
		ID:     "start-1",
		Labels: []string{"Entry"},
		Properties: map[string]interface{}{
			"score": int64(85),
			"name":  "Test Entry",
		},
	}
	endNode := &storage.Node{
		ID:     "end-1",
		Labels: []string{"IssueType"},
		Properties: map[string]interface{}{
			"name": "Informal Register (tú)",
		},
	}
	rel := &storage.Edge{
		ID:        "rel-1",
		Type:      "HAS_ISSUE",
		StartNode: "start-1",
		EndNode:   "end-1",
	}

	ctx := PathContext{
		nodes: map[string]*storage.Node{
			"e": startNode,
			"i": endNode,
		},
		rels: map[string]*storage.Edge{
			"r": rel,
		},
	}

	tests := []struct {
		name        string
		whereClause string
		expected    bool
	}{
		{
			name:        "simple string equality",
			whereClause: "i.name = 'Informal Register (tú)'",
			expected:    true,
		},
		{
			name:        "string equality - no match",
			whereClause: "i.name = 'Other Issue'",
			expected:    false,
		},
		{
			name:        "numeric less than",
			whereClause: "e.score < 90",
			expected:    true,
		},
		{
			name:        "numeric greater than - no match",
			whereClause: "e.score > 90",
			expected:    false,
		},
		{
			name:        "AND condition - both true",
			whereClause: "e.score < 90 AND i.name = 'Informal Register (tú)'",
			expected:    true,
		},
		{
			name:        "AND condition - one false",
			whereClause: "e.score > 90 AND i.name = 'Informal Register (tú)'",
			expected:    false,
		},
		{
			name:        "OR condition - one true",
			whereClause: "e.score > 90 OR i.name = 'Informal Register (tú)'",
			expected:    true,
		},
		{
			name:        "IS NOT NULL",
			whereClause: "i.name IS NOT NULL",
			expected:    true,
		},
		{
			name:        "CONTAINS",
			whereClause: "i.name CONTAINS 'Register'",
			expected:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.evaluateWhereOnPath(tt.whereClause, ctx)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestFilterPathsByWhere tests filtering paths with WHERE clause
func TestFilterPathsByWhere(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	// Create test nodes
	entry1 := &storage.Node{
		ID:         "entry-1",
		Labels:     []string{"Entry"},
		Properties: map[string]interface{}{"score": int64(80)},
	}
	entry2 := &storage.Node{
		ID:         "entry-2",
		Labels:     []string{"Entry"},
		Properties: map[string]interface{}{"score": int64(95)},
	}
	issue1 := &storage.Node{
		ID:         "issue-1",
		Labels:     []string{"IssueType"},
		Properties: map[string]interface{}{"name": "Informal Register (tú)"},
	}
	issue2 := &storage.Node{
		ID:         "issue-2",
		Labels:     []string{"IssueType"},
		Properties: map[string]interface{}{"name": "Other Issue"},
	}

	// Create paths
	paths := []PathResult{
		{
			Nodes:         []*storage.Node{entry1, issue1},
			Relationships: []*storage.Edge{{ID: "r1", Type: "HAS_ISSUE"}},
		},
		{
			Nodes:         []*storage.Node{entry2, issue2},
			Relationships: []*storage.Edge{{ID: "r2", Type: "HAS_ISSUE"}},
		},
	}

	matches := &TraversalMatch{
		StartNode:    nodePatternInfo{variable: "e", labels: []string{"Entry"}},
		EndNode:      nodePatternInfo{variable: "i", labels: []string{"IssueType"}},
		Relationship: RelationshipPattern{Variable: "r", Types: []string{"HAS_ISSUE"}},
	}

	t.Run("filter by end node property", func(t *testing.T) {
		filtered := exec.filterPathsByWhere(paths, matches, "i.name = 'Informal Register (tú)'")
		assert.Len(t, filtered, 1)
	})

	t.Run("filter by start node property", func(t *testing.T) {
		filtered := exec.filterPathsByWhere(paths, matches, "e.score < 90")
		assert.Len(t, filtered, 1)
	})

	t.Run("no filter", func(t *testing.T) {
		filtered := exec.filterPathsByWhere(paths, matches, "")
		assert.Len(t, filtered, 2)
	})

	t.Run("filter all out", func(t *testing.T) {
		filtered := exec.filterPathsByWhere(paths, matches, "e.score > 100")
		assert.Len(t, filtered, 0)
	})
}
