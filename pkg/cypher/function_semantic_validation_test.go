package cypher

import "testing"

func TestReturnRejectsUnknownFunction(t *testing.T) {
	err := validateKnownFunctions("MATCH (node) RETURN unknownFunction(node)")
	requireSemanticDetail(t, err, "UnknownFunction")
}

func TestReturnAllowsNestedKnownFunctions(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	_, _, err := exec.enrichBoundTypes(ctx, "RETURN toString(size(labels(node)))", typeScope{"node": provenOperand("Node")}, nil, "", false)
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
