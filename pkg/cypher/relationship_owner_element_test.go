package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRelationshipDeclarationElementBoundary(t *testing.T) {
	var elements []patternElement
	eachPatternElement("(a)-[`r``r`:`A|B` {k: 7} WHERE true]->(b)", func(element patternElement) {
		elements = append(elements, element)
	})
	require.Len(t, elements, 3)
	require.Equal(t, "(a)", elements[0].node)
	require.False(t, elements[0].isRelationship)
	require.Equal(t, "(b)", elements[2].node)
	require.True(t, elements[1].isRelationship)
	require.Empty(t, elements[1].node, "relationship source must never leave the owner through the node field")
	require.Equal(t, "`r``r`", elements[1].relationship.variable)
	require.Equal(t, []string{"A|B"}, elements[1].relationship.types.storageNames())
	require.Equal(t, "{k: 7}", elements[1].relationship.properties)
	require.Equal(t, "true", elements[1].relationship.predicate)
}

func TestRelationshipOwnerDeclaredVariables(t *testing.T) {
	variables := declaredPatternVariables("((a {m: COUNT { (hidden)-[ignored]->() }})-[`r``r`:$($type) WHERE true]->(b))")
	require.Equal(t, map[string]bool{"a": true, "r`r": true, "b": true}, variables)
}

func TestRelationshipOwnerQuotedBindingInventory(t *testing.T) {
	query := "MATCH ()-[outer {k: COUNT { ()-[true]->() WHERE type(`true`) = 'T' }} WHERE EXISTS { ()-[is]->() }]->() RETURN `outer`"
	bindings := relationshipVariableTokens(query)
	var names []string
	for _, binding := range bindings {
		require.Equal(t, binding.written, query[binding.start:binding.end])
		require.True(t, binding.binding)
		names = append(names, binding.name)
	}
	require.Equal(t, []string{"outer", "true", "is"}, names)
	require.Empty(t, relationshipVariableTokens("RETURN -[x], `x`, '-[y]'"), "a negative list expression declares no relationship")
	require.Empty(t, relationshipVariableTokens("RETURN 1 /* ()-[fake]->() */"), "a comment declares no binding")
}

func TestRelationshipOwnerTraversalParts(t *testing.T) {
	parts := traversalPatternParts("(a) - [r:`A|B`*1..2] -> (b)")
	require.Len(t, parts, 2)
	require.True(t, parts[0].hasRelationship)
	require.False(t, parts[1].hasRelationship)
	require.True(t, parts[0].relationship.outgoing)
	require.Equal(t, []string{"A|B"}, parts[0].relationship.types.storageNames())
	require.Equal(t, "r", parts[0].relationship.variable)
}
