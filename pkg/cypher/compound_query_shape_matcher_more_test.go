package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompoundQueryShapeMatcher_MoreRejectBranches(t *testing.T) {
	tests := []struct {
		name   string
		query  string
		reason string
	}{
		{"bad_left_node", "MATCH n, (b:B) WITH n LIMIT 1 CREATE (n)-[r:REL]->(b) DELETE r", "invalid left"},
		{"bad_right_node", "MATCH (a:A), b WITH a LIMIT 1 CREATE (a)-[r:REL]->(b) DELETE r", "invalid right"},
		{"bad_create_clause", "MATCH (a:A), (b:B) WITH a LIMIT 1 CREATE (a)-[:REL]->(b) DELETE r", "invalid CREATE"},
		{"missing_delete_var", "MATCH (a:A), (b:B) WITH a LIMIT 1 CREATE (a)-[r:REL]->(b) DELETE   ", "missing DELETE"},
		{"invalid_limit", "MATCH (a:A), (b:B) WITH a LIMIT -1 CREATE (a)-[r:REL]->(b) DELETE r", "invalid LIMIT"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, ok := matchCompoundCreateDeleteRelShape(tt.query)
			require.False(t, ok)
			require.Contains(t, m.Probe.RejectReason, tt.reason)
		})
	}

	// Explicitly hit missing top-level comma split and missing LIMIT-literal branches.
	m, ok := matchCompoundCreateDeleteRelShape("MATCH (a:A) WITH a LIMIT 1 CREATE (a)-[r:REL]->(b) DELETE r")
	require.False(t, ok)
	require.Contains(t, m.Probe.RejectReason, "expected two MATCH")
	m, ok = matchCompoundCreateDeleteRelShape("MATCH (a:A), (b:B) WITH a LIMIT   CREATE (a)-[r:REL]->(b) DELETE r")
	require.False(t, ok)
	require.Contains(t, m.Probe.RejectReason, "missing LIMIT")

	propTests := []struct {
		name   string
		query  string
		reason string
	}{
		{"bad_match_split", "MATCH (a:A {id:1}) CREATE (a)-[r:R]->(b) DELETE r", "expected two MATCH"},
		{"bad_left_pattern", "MATCH a, (b:B {id:2}) CREATE (a)-[r:R]->(b) DELETE r", "invalid left"},
		{"bad_delete", "MATCH (a:A {id:1}), (b:B {id:2}) CREATE (a)-[r:R]->(b) DELETE", "missing DELETE"},
	}
	for _, tt := range propTests {
		t.Run(tt.name, func(t *testing.T) {
			m, ok := matchCompoundPropCreateDeleteRelShape(tt.query)
			require.False(t, ok)
			require.Contains(t, m.Probe.RejectReason, tt.reason)
		})
	}
	m, ok = matchCompoundPropCreateDeleteRelShape("MATCH (a:A {id:1}), b CREATE (a)-[r:R]->(b) DELETE r")
	require.False(t, ok)
	require.Contains(t, m.Probe.RejectReason, "invalid right")

	countTests := []struct {
		name   string
		query  string
		reason string
	}{
		{"missing_with_var", "MATCH (a:A {id:1}), (b:B {id:2}) CREATE (a)-[r:R]->(b) WITH   DELETE r RETURN count(r)", "shape not found"}, // DELETE after WITH is a name (#740); the shape is rejected either way
		{"missing_delete_var", "MATCH (a:A {id:1}), (b:B {id:2}) CREATE (a)-[r:R]->(b) WITH r DELETE   RETURN count(r)", "missing DELETE"},
		{"bad_return_count", "MATCH (a:A {id:1}), (b:B {id:2}) CREATE (a)-[r:R]->(b) WITH r DELETE r RETURN r", "not COUNT"},
		{"missing_count_var", "MATCH (a:A {id:1}), (b:B {id:2}) CREATE (a)-[r:R]->(b) WITH r DELETE r RETURN count()", "missing COUNT"},
	}
	for _, tt := range countTests {
		t.Run(tt.name, func(t *testing.T) {
			m, ok := matchCompoundPropCreateDeleteReturnCountRelShape(tt.query)
			require.False(t, ok)
			require.Contains(t, m.Probe.RejectReason, tt.reason)
		})
	}
	m, ok = matchCompoundPropCreateDeleteReturnCountRelShape("MATCH (a:A {id:1}) CREATE (a)-[r:R]->(b) WITH r DELETE r RETURN count(r)")
	require.False(t, ok)
	require.Contains(t, m.Probe.RejectReason, "expected two MATCH")
	m, ok = matchCompoundPropCreateDeleteReturnCountRelShape("MATCH a, (b:B {id:2}) CREATE (a)-[r:R]->(b) WITH r DELETE r RETURN count(r)")
	require.False(t, ok)
	require.Contains(t, m.Probe.RejectReason, "invalid left")
	m, ok = matchCompoundPropCreateDeleteReturnCountRelShape("MATCH (a:A {id:1}), b CREATE (a)-[r:R]->(b) WITH r DELETE r RETURN count(r)")
	require.False(t, ok)
	require.Contains(t, m.Probe.RejectReason, "invalid right")
	m, ok = matchCompoundPropCreateDeleteReturnCountRelShape("MATCH (a:A {id:1}), (b:B {id:2}) CREATE (a)-[:R]->(b) WITH r DELETE r RETURN count(r)")
	require.False(t, ok)
	require.Contains(t, m.Probe.RejectReason, "invalid CREATE")
}

func TestCompoundQueryShapeMatcher_MoreHelperParserBranches(t *testing.T) {
	// splitTopLevelCommaShape handles bracket depth and empty sides.
	_, _, ok := splitTopLevelCommaShape(", (b:B)")
	require.False(t, ok)
	_, _, ok = splitTopLevelCommaShape("(a:A),")
	require.False(t, ok)
	left, right, ok := splitTopLevelCommaShape("(a:A {v:[1,2]}), (b:B {v:'x,y'})")
	require.True(t, ok)
	require.Equal(t, "(a:A {v:[1,2]})", left)
	require.Equal(t, "(b:B {v:'x,y'})", right)

	// parseLabeledNodePattern and property parsing rejects malformed bodies.
	_, ok = parseLabeledNodePattern("(n:User {id})")
	require.False(t, ok)
	_, ok = parseLabeledNodePattern("(n: {id:1})")
	require.False(t, ok)
	_, _, ok = parseSinglePropertyAssignment(": 1")
	require.False(t, ok)

	// parseCreateRelationshipClause rejects missing syntax pieces.
	_, ok = parseCreateRelationshipClause("CREATE a-[r:R]->(b)")
	require.False(t, ok)
	_, ok = parseCreateRelationshipClause("CREATE (a)-[r:R](b)")
	require.False(t, ok)
	_, ok = parseCreateRelationshipClause("CREATE (a)-[r:R]->(b) RETURN b")
	require.False(t, ok)
	_, ok = parseCreateRelationshipClause("CREATE (a)-[r]->(b)")
	require.False(t, ok)
	_, ok = parseCreateRelationshipClause("CREATE (a)-[:R]->(b)")
	require.False(t, ok)
	_, ok = parseCreateRelationshipClause("CREATE (a)-[r:R]->")
	require.False(t, ok)

	// parseBareNodeReference/extract helpers with quotes and malformed delimiters.
	_, _, ok = parseBareNodeReference("(a b)")
	require.False(t, ok)
	inside, rest, ok := extractParenSection("(')')tail")
	require.True(t, ok)
	require.Equal(t, "')'", inside)
	require.Equal(t, "tail", rest)
	_, _, ok = extractParenSection("nope")
	require.False(t, ok)
	inside, rest, ok = extractBracketSection("[\"]\"]tail")
	require.True(t, ok)
	require.Equal(t, "\"]\"", inside)
	require.Equal(t, "tail", rest)
	_, _, ok = extractBracketSection("oops")
	require.False(t, ok)
}

func TestCompoundQueryShapeMatcher_CountReturnFormattingBranches(t *testing.T) {
	q := "MATCH (a:A {id:1}), (b:B {id:2}) CREATE (a)-[r:REL]->(b) WITH r DELETE r RETURN \t count(r)  "
	m, ok := matchCompoundPropCreateDeleteReturnCountRelShape(q)
	require.True(t, ok)
	require.True(t, m.Probe.Matched)
	require.Equal(t, "r", m.Captures.String("count_var"))
}
