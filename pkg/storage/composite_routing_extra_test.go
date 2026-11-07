package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryAnalyzer_AnalyzeQuery(t *testing.T) {
	a := NewQueryAnalyzer()
	for _, tc := range []struct {
		name, query             string
		writes, fullScan, label bool
	}{
		{"explicit label token", "MATCH :Person RETURN n", false, false, true},
		{"parenthesized label token", "MATCH ( :Person ) RETURN n", false, false, true},
		{"resolved write", "CREATE (n:Person {name: 'a'})", true, false, false},
		{"conventional match hint", "MATCH (n:Person) RETURN n", false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := a.AnalyzeQuery(tc.query, tc.writes)
			assert.Equal(t, tc.writes, info.IsWrite)
			assert.Equal(t, tc.fullScan, info.IsFullScan)
			if tc.label {
				assert.Contains(t, info.Labels, "PERSON")
			}
		})
	}
}

// TestQueryAnalyzer_AnalyzeQuery_ResolvedEffect pins the canonical effect
// boundary: AnalyzeQuery reports the effect its caller resolved and never
// reclassifies the statement text, while shape extraction stays unchanged.
func TestQueryAnalyzer_AnalyzeQuery_ResolvedEffect(t *testing.T) {
	a := NewQueryAnalyzer()
	for _, tc := range []struct {
		name, query             string
		writes                  bool
		wantWrite, wantFullScan bool
	}{
		{"mutation resolved as write", "CREATE (n:Doc {name: 'a'})", true, true, false},
		{"mutation resolved as read", "CREATE (n:Doc {name: 'a'})", false, false, true},
		{"read resolved as read", "MATCH (n) RETURN n", false, false, true},
		{"read resolved as write", "MATCH (n) RETURN n", true, true, false},
		{"quoted keyword resolved as read", "RETURN 'DELETE n' AS text", false, false, true},
		{"quoted keyword resolved as write", "RETURN 'DELETE n' AS text", true, true, false},
		{"labelled read resolved as write", "MATCH :Person RETURN n", true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := a.AnalyzeQuery(tc.query, tc.writes)
			assert.Equal(t, tc.wantWrite, info.IsWrite, "the resolved effect is authoritative")
			assert.Equal(t, tc.wantFullScan, info.IsFullScan)
		})
	}
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
