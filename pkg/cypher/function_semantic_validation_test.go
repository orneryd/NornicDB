package cypher

import "testing"

func TestReturnRejectsUnknownFunction(t *testing.T) {
	err := validateReturnSemanticScope(matchSemanticScope{"node": matchBindingNode}, "RETURN unknownFunction(node)")
	requireSemanticDetail(t, err, "UnknownFunction")
}

func TestReturnAllowsNestedKnownFunctions(t *testing.T) {
	err := validateReturnSemanticScope(matchSemanticScope{"node": matchBindingNode}, "RETURN toString(size(labels(node)))")
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
		if err := validateKnownFunctionsInExpression(expression); err != nil {
			t.Fatalf("%q produced an unexpected semantic error: %v", expression, err)
		}
	}
}
