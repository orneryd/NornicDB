package cypher

import "testing"

// TestReplaceLabeledNodePatternVariable: the (variable:Labels) rewrite
// replaces the variable as the pattern it replaced did (#591), leaving
// other variables and patterns without labels as they are.
func TestReplaceLabeledNodePatternVariable(t *testing.T) {
	for input, want := range map[string]string{
		"MATCH (p:Person)-[:KNOWS]->(f) RETURN f":        "MATCH (id1:Person)-[:KNOWS]->(f) RETURN f",
		"MATCH (p:Person:Admin), (p:Team) RETURN 1":      "MATCH (id1:Person:Admin), (id1:Team) RETURN 1",
		"MATCH (pp:Person), (p) RETURN p":                "MATCH (pp:Person), (p) RETURN p",
		"MATCH (p:) RETURN p":                            "MATCH (p:) RETURN p",
		"MATCH (p:Person RETURN p":                       "MATCH (p:Person RETURN p",
		"MATCH (p:Person {id: 1})-->(q:Person) RETURN q": "MATCH (id1:Person {id: 1})-->(q:Person) RETURN q",
	} {
		if got := replaceLabeledNodePatternVariable(input, "p", "id1"); got != want {
			t.Errorf("%q: got %q, want %q", input, got, want)
		}
	}
}
