package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A quantifier or reduce whose arguments hold a subquery is one scope unit;
// other calls and text are not.
func TestVariableScopeCallAt(t *testing.T) {
	for expr, want := range map[string]string{
		"all(v IN l WHERE EXISTS { MATCH (n) WHERE n.id = v })":   "QUANTIFIER",
		"SINGLE (v IN l WHERE COUNT { MATCH (n {id: v}) } = 1)":   "QUANTIFIER",
		"reduce(s = 0, v IN l | s + COUNT { MATCH (n {id: v}) })": "REDUCE",
		"all(v IN l WHERE v > 1)":                                 "",
		"all(v IN l)":                                             "",
		"any(v IN [{count: 1}] WHERE v.count = 1)":                "",
		"any(v IN l WHERE EXISTS { MATCH (n) }":                   "",
		"anything(v IN l WHERE EXISTS { MATCH (n) })":             "",
		"none":       "",
		"single + 1": "",
	} {
		scope, ok := variableScopeCallAt(expr, 0)
		require.Equal(t, want != "", ok, expr)
		require.Equal(t, want, scope.kind, expr)
	}
	for _, expr := range []string{"n.all(v IN l WHERE EXISTS { MATCH (n) })", "$all(v IN l WHERE EXISTS { MATCH (n) })", "xall(v IN l WHERE EXISTS { MATCH (n) })"} {
		_, ok := variableScopeCallAt(expr, 2)
		require.False(t, ok, expr)
	}
}

func TestSubqueryPropertyMapReadsRowValue(t *testing.T) {
	values := map[string]interface{}{"r": map[string]interface{}{"prop": 1}}
	require.True(t, subqueryPropertyMapReadsRowValue("(n {id: r.prop})", values))
	require.True(t, subqueryPropertyMapReadsRowValue("()-[x {id: r.prop}]->()", values))
	require.False(t, subqueryPropertyMapReadsRowValue("(n {id: 1}) WHERE n.s = '(r {'", values))
	require.False(t, subqueryPropertyMapReadsRowValue("(n {id: $r})-->(m {k: 1})", map[string]interface{}{"x": 1}))
	require.False(t, subqueryPropertyMapReadsRowValue("(n {id: r.prop", values))
}
