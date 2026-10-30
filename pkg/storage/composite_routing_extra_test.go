package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryAnalyzer_AnalyzeQuery(t *testing.T) {
	a := NewQueryAnalyzer()

	// Label extraction in this lightweight analyzer expects explicit ":Label" tokens.
	readWithLabelToken := a.AnalyzeQuery("MATCH :Person RETURN n")
	assert.Contains(t, readWithLabelToken.Labels, "PERSON")
	assert.False(t, readWithLabelToken.IsWrite)
	assert.False(t, readWithLabelToken.IsFullScan)

	write := a.AnalyzeQuery("CREATE (n:Person {name: 'a'})")
	assert.True(t, write.IsWrite)

	// Typical MATCH syntax without an explicit :Label token is treated as full scan.
	scan := a.AnalyzeQuery("MATCH (n:Person) RETURN n")
	assert.True(t, scan.IsFullScan)
}

func TestQueryAnalyzer_RouteQuery_LabelRouting(t *testing.T) {
	a := NewQueryAnalyzer()
	a.SetLabelRouting("Person", []string{"dbA"})
	a.SetLabelRouting("Company", []string{"dbB", "dbC"})

	q1 := &QueryInfo{Labels: []string{"Person"}, Properties: map[string]interface{}{}}
	r1 := a.RouteQuery(q1, []string{"dbA", "dbB", "dbC"})
	assert.Equal(t, []string{"dbA"}, r1)

	q2 := &QueryInfo{Labels: []string{"Company"}, Properties: map[string]interface{}{}}
	r2 := a.RouteQuery(q2, []string{"dbA", "dbB", "dbC"})
	assert.ElementsMatch(t, []string{"dbB", "dbC"}, r2)

	q3 := &QueryInfo{Labels: []string{"Unknown"}, Properties: map[string]interface{}{}}
	r3 := a.RouteQuery(q3, []string{"dbA", "dbB", "dbC"})
	assert.Equal(t, []string{"dbA", "dbB", "dbC"}, r3)
}

func TestQueryAnalyzer_AnalyzeQuery_ParenLabel(t *testing.T) {
	a := NewQueryAnalyzer()

	// Test the ( :Label pattern — triggers the uncovered branch at line 73
	// where a label follows "(" or ","
	q := a.AnalyzeQuery("MATCH ( :Person ) RETURN n")
	assert.Contains(t, q.Labels, "PERSON")
	assert.False(t, q.IsWrite)
}

func TestQueryAnalyzer_RouteQuery_PropertyRouting(t *testing.T) {
	a := NewQueryAnalyzer()
	a.SetPropertyRouting("tenant", "a", "dbA")
	a.SetPropertyRouting("tenant", "b", "dbB")
	a.SetPropertyDefault("tenant", "dbDefault")

	q1 := &QueryInfo{Labels: nil, Properties: map[string]interface{}{"tenant": "a"}}
	r1 := a.RouteQuery(q1, []string{"dbA", "dbB", "dbDefault"})
	assert.Equal(t, []string{"dbA"}, r1)

	q2 := &QueryInfo{Labels: nil, Properties: map[string]interface{}{"tenant": "x"}}
	r2 := a.RouteQuery(q2, []string{"dbA", "dbB", "dbDefault"})
	assert.Equal(t, []string{"dbDefault"}, r2)
}
