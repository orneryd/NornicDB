package antlr

import "testing"

// Kept public parse trees must not pin a prediction generation after parsing.
// This regression uses only the existing Parse/tree API, including error trees.
func TestPredictionReleaseTreeDetachesInterpreter(t *testing.T) {
	for _, query := range []string{"RETURN 1 AS value", "RETURN"} {
		result, _ := Parse(query)
		if result == nil || result.Tree == nil {
			t.Fatalf("query %q returned no tree", query)
		}
		if result.Tree.GetParser().GetInterpreter() != nil {
			t.Errorf("query %q left the completed parse tree retaining prediction state", query)
		}
	}
}
