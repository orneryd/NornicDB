package cypher

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// staticTypeChoices is a stateless reader: every read names the same types in
// the same order, splitting only at the top level of a static type union and
// keeping a nested List<...> whole.
func TestStaticTypeChoicesReadsTopLevelUnion(t *testing.T) {
	for _, testCase := range []struct {
		typeName string
		choices  []string
	}{
		{"", []string{""}},
		{"Integer", []string{"Integer"}},
		{"Float or Integer", []string{"Float", "Integer"}},
		{"Float, Integer or String", []string{"Float", "Integer", "String"}},
		{"List<Float or Integer>", []string{"List<Float or Integer>"}},
		{"List<Map>, List<Node> or List<Relationship>", []string{"List<Map>", "List<Node>", "List<Relationship>"}},
		{"List<List<Float, Integer>> or String", []string{"List<List<Float, Integer>>", "String"}},
		{"Float, ", []string{"Float", ""}},
		{"List<T> or Integer", []string{"List<T>", "Integer"}},
	} {
		want := testCase.choices
		require.Equal(t, want, slices.Collect(staticTypeChoices(testCase.typeName)), testCase.typeName)
		// The reader holds no state: a second read yields the same types.
		require.Equal(t, want, slices.Collect(staticTypeChoices(testCase.typeName)), testCase.typeName)
		require.Equal(t, len(want), staticTypeChoiceCount(testCase.typeName), testCase.typeName)
	}
	require.True(t, staticTypeChoiceNamed("Float or Integer", "Integer"))
	require.False(t, staticTypeChoiceNamed("Float or Integer", "String"))
	// A nested union's separator names no alternative of its own.
	require.False(t, staticTypeChoiceNamed("List<Float or Integer>", "Integer"))
}

// A caller that appends to or rewrites its alternatives keeps its own copy:
// no caller's materialized slice is another caller's storage, and a rewrite
// never reaches the next reader.
func TestStaticTypeChoicesMaterializedAlternativesAreIsolated(t *testing.T) {
	const union = "Map, Node or Relationship"
	alternatives := []string{"Map", "Node", "Relationship"}
	require.Equal(t, alternatives, slices.Collect(staticTypeChoices(union)))
	require.Equal(t, "List<Map>, List<Node> or List<Relationship>", listOfType(union))

	owned := slices.Collect(staticTypeChoices(union))
	owned[0] = "Rewritten"
	require.Equal(t, alternatives, slices.Collect(staticTypeChoices(union)))
	require.Equal(t, "List<Map>, List<Node> or List<Relationship>", listOfType(union))
	require.Equal(t, union, typedOperand(union).typeName())

	joined := joinedOperand(typedOperand("Float or Integer").proved(), typedOperand("String").proved())
	require.Equal(t, "Float, Integer or String", joined.typeName())
	require.Equal(t, "Float or Integer", typedOperand("Float or Integer").proved().typeName())
	require.Equal(t, "String", typedOperand("String").proved().typeName())
}
