package antlr

import "testing"

func TestConstraintPatternCanonicalFacts(t *testing.T) {
	for _, text := range []string{
		"(n)-[:R]->(:End)", "(n)-[:`$(R)`]->(:`End|One`)",
		"(n)<-[:`A``B`]-(:`中``文`)", "(n)-[:`A}B`]->(:`End]One`)",
	} {
		if _, ok := ParseConstraintPattern(text); !ok {
			t.Errorf("valid contract pattern rejected: %s", text)
		}
	}
	for _, text := range []string{
		"(n)-[:R|S]->()", "(n)-[:$(t)]->()", "(n)-[:R*]->()",
		"(n)-[r:R]->()", "(n)-[:R]-(m)", "(n)<-[:R]->(m)",
		"(n)-[:`R`*1]->()", "(n)-[:`R` WHERE true]->()", "(n)-[:`R` {}]->()", "(n)-[:`R`junk]->()",
		"(n)-[:R]->(:!End)", "(n)-[:R]->(m) RETURN m", "(n)-[:R]->(m)-[:R]->()",
	} {
		if _, ok := ParseConstraintPattern(text); ok {
			t.Errorf("unsupported contract form admitted: %s", text)
		}
	}
	pattern, ok := ParseConstraintPattern("(n)-[:`$(R)`]->(:`End|One`)")
	if !ok || !pattern.MatchesType("$(R)") || pattern.MatchesType("R") || !pattern.MatchesTargetLabels([]string{"End|One"}) || pattern.MatchesTargetLabels([]string{"End", "One"}) {
		t.Fatal("literal identity was erased")
	}
}

func TestConstraintPatternPreservesKeywordIdentifiers(t *testing.T) {
	for _, name := range []string{"is", "where", "as", "any", "all", "none", "single", "not", "and", "or", "xor", "in", "contains", "starts", "ends", "null", "true", "false", "count", "exists", "case", "when"} {
		text := "(" + name + ")-[:" + name + "]->(:" + name + ")"
		pattern, ok := ParseConstraintPattern(text)
		if !ok || !pattern.MatchesType(name) || !pattern.MatchesTargetLabels([]string{name}) {
			t.Errorf("accepted contract identifier lost: %s", name)
		}
	}
}
