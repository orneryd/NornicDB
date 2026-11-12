package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNativeInputTypeProofIncludesNestedEntities(t *testing.T) {
	for _, tc := range []struct {
		name     string
		typeInfo staticOperand
		entity   bool
	}{
		{"unknown", staticOperand{}, true},
		{"aggregate hint", staticOperand{nonEntity: true, proven: true}, true},
		{"unproven scalar", knownOperand("String"), true},
		{"property", staticOperand{display: staticPropertyValueType, proven: true}, false},
		{"node", provenOperand("Node"), true},
		{"relationship", provenOperand("Relationship"), true},
		{"path", provenOperand("Path"), true},
		{"map", provenOperand("Map"), true},
		{"unknown list", provenOperand("List<Any>"), true},
		{"node list", provenOperand("List<Node>"), true},
		{"nested node list", provenOperand("List<List<Node>>"), true},
		{"string", provenOperand("String"), false},
		{"null", provenOperand("Null"), false},
		{"scalar list", provenOperand("List<String>"), false},
		{"nested scalar list", provenOperand("List<List<Integer>>"), false},
		{"scalar union", signatureOperand([]string{"Integer", "String"}), false},
		{"entity union", signatureOperand([]string{"Integer", "Node"}), true},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.entity, tc.typeInfo.mayReachGraphEntity()) })
	}
}
