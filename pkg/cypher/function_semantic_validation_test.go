// Author of retained-input changes: Codex R17 implementation agent, 29 October 2026.
package cypher

import "testing"

func TestReturnRejectsUnknownFunction(t *testing.T) {
	err := validateKnownFunctions("MATCH (node) RETURN unknownFunction(node)")
	requireSemanticDetail(t, err, "UnknownFunction")
}

func TestReturnAllowsNestedKnownFunctions(t *testing.T) {
	err := validateReturnSemanticScope(t.Context(), matchSemanticScope{"node": matchBindingNode}, retainedClauseForTest(t.Context(), "RETURN toString(size(labels(node)))"))
	if err != nil {
		t.Fatalf("unexpected semantic error: %v", err)
	}
}

func TestReturnDoesNotTreatParenthesizedBooleanOperandsAsFunctions(t *testing.T) {
	for _, expression := range []string{
		"true AND (false AND true)",
		"false OR (true XOR false)",
		"NOT (true AND false)",
	} {
		if err := validateKnownFunctions(expression); err != nil {
			t.Fatalf("%q produced an unexpected semantic error: %v", expression, err)
		}
	}
}
