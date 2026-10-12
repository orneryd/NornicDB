package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTraversalVariablePropertiesRead(t *testing.T) {
	tests := []struct {
		expression string
		read       bool
	}{
		{"count(l)", false},
		{"count(DISTINCT l)", false},
		{"COUNT(l)", false},
		{"id(l)", false},
		{"elementId(l)", false},
		{"labels(l)", false},
		{"count(l) + 1", false},
		{"l", true},
		{"l.name", true},
		{"l.name = 'x'", true},
		{"collect(l)", true},
		{"size(collect(l))", true},
		{"l.age > 30", true},
		{"l IS NOT NULL", true},
		{"head([l.id])", true},
		{"other.name", false}, // different variable
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.expression, func(t *testing.T) {
			require.Equal(t, tt.read, traversalVariablePropertiesRead(tt.expression, "l"))
		})
	}
}

func TestTraversalUnusedPropertyVariables(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	items := []returnItem{{expr: "count(l)", alias: "c"}}

	// Non-chained: only the end variable matters.
	match := &TraversalMatch{
		StartNode: nodePatternInfo{variable: "a", labels: []string{"P"}},
		EndNode:   nodePatternInfo{variable: "l", labels: []string{"Q"}},
	}
	unused := e.traversalUnusedPropertyVariables(match, items, "")
	require.True(t, unused["l"])
	require.True(t, unused["a"], "start variable is also unused but never read by traversal endpoints")

	// A property read flips that variable back to "read"; other variables stay
	// as they were.
	unused = e.traversalUnusedPropertyVariables(match, []returnItem{{expr: "l.text", alias: "t"}}, "")
	require.False(t, unused["l"], "l.text is a property read")
	require.True(t, unused["a"], "a's properties are still unread")

	// Chained: intermediate node variables are analysed too.
	chained := &TraversalMatch{
		IsChained: true,
		StartNode: nodePatternInfo{variable: "r", labels: []string{"Root"}},
		EndNode:   nodePatternInfo{variable: "l", labels: []string{"Leaf"}},
		Segments: []TraversalSegment{
			{FromNode: nodePatternInfo{variable: "r", labels: []string{"Root"}}, ToNode: nodePatternInfo{variable: "c", labels: []string{"Child"}}},
			{FromNode: nodePatternInfo{variable: "c", labels: []string{"Child"}}, ToNode: nodePatternInfo{variable: "l", labels: []string{"Leaf"}}},
		},
	}
	unused = e.traversalUnusedPropertyVariables(chained, items, "")
	require.True(t, unused["l"])
	require.True(t, unused["c"])
	require.True(t, unused["r"])

	// The WHERE predicate counts against a variable.
	unused = e.traversalUnusedPropertyVariables(chained, items, "c.id = 3")
	require.False(t, unused["c"])
	require.True(t, unused["l"])
}
