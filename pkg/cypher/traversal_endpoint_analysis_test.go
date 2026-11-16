package cypher

import (
	"context"
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
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
	unused := e.traversalUnusedPropertyVariables(t.Context(), match, items, "")
	require.True(t, unused["l"])
	require.True(t, unused["a"], "start variable is also unused but never read by traversal endpoints")

	// A property read flips that variable back to "read"; other variables stay
	// as they were.
	unused = e.traversalUnusedPropertyVariables(t.Context(), match, []returnItem{{expr: "l.text", alias: "t"}}, "")
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
	unused = e.traversalUnusedPropertyVariables(t.Context(), chained, items, "")
	require.True(t, unused["l"])
	require.True(t, unused["c"])
	require.True(t, unused["r"])

	// The WHERE predicate counts against a variable.
	unused = e.traversalUnusedPropertyVariables(t.Context(), chained, items, "c.id = 3")
	require.False(t, unused["c"])
	require.True(t, unused["l"])
}

// A full-scope registration anywhere in the statement's consumers keeps every
// binding this MATCH materialises: the callback reads the row, not only the
// arguments it is given, so no variable is header-only-safe even when nothing
// names its properties. The registration is a name of its own, so the suite's
// other registrations stay untouched.
func TestTraversalUnusedPropertyVariablesKeepsEveryBindingForFullScopeCallback(t *testing.T) {
	cypherfn.Register("traversal_scope_probe", func(_ context.Context, _ cypherfn.Context, _ []string) (interface{}, error) {
		return nil, nil
	}, cypherfn.OpaqueReads, cypherfn.FullInputImage)
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	match := &TraversalMatch{
		StartNode: nodePatternInfo{variable: "a", labels: []string{"P"}},
		EndNode:   nodePatternInfo{variable: "l", labels: []string{"Q"}},
	}
	items := []returnItem{{expr: "count(l)", alias: "c"}}
	require.True(t, e.traversalUnusedPropertyVariables(t.Context(), match, items, "")["l"],
		"a header aggregate narrows the binding it counts")

	items = append(items, returnItem{expr: "traversal_scope_probe('x')", alias: "p"})
	require.Nil(t, e.traversalUnusedPropertyVariables(t.Context(), match, items, ""),
		"a full-scope callback keeps every visible binding's whole body")
}
