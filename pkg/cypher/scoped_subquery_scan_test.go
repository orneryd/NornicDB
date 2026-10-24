package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// boundChainCount's traversal matches literal and parameter property maps
// only; a map value that reads a variable, bound in the row or not, is left
// to the kernel or the pipeline (#1055).
func TestPatternMapsAreConstant(t *testing.T) {
	for pattern, want := range map[string]bool{
		"(n)-[:R]->(m)":                  true,
		"(n {id: 1})-[:R {k: $k}]->(m)":  true,
		"(n {s: 'a {b} c', l: [1, 2]})":  true,
		"(n)-[:R]->(m {id: o.id})":       false,
		"(n {id: i + 1})-->()":           false,
		"(n {id: r.ids[0]})":             false,
		"()-[x {id: prop}]->()":          false,
		"(n {id: 1}) WHERE n.s = '(r {'": true,
		"(n {id: r.prop":                 false,
	} {
		require.Equal(t, want, patternMapsAreConstant(pattern), pattern)
	}
}

// boundChainCount compares a traversal with the row's nodes and
// relationships: a chain variable the row binds to anything else is left to
// the kernel or the pipeline.
func TestChainVariablesBindEntities(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "chainvars"))
	ctx := context.Background()
	fixtureAdmission855Context, fixtureAdmission855 := traversalTestContext(ctx, "(n)-[r:R]->(m)", "", nil)
	_ = fixtureAdmission855
	matches := exec.parseTraversalPattern(fixtureAdmission855, fixtureAdmission855Context, "(n)-[r:R]->(m)")
	require.NotNil(t, matches)
	node, edge := &storage.Node{ID: "n1"}, &storage.Edge{ID: "e1"}
	require.True(t, chainVariablesBindEntities(matches, map[string]interface{}{"n": node}))
	require.True(t, chainVariablesBindEntities(matches, map[string]interface{}{"n": node, "r": edge, "m": node}))
	require.False(t, chainVariablesBindEntities(matches, map[string]interface{}{"n": node, "r": "ab"}))
	require.False(t, chainVariablesBindEntities(matches, map[string]interface{}{"n": node, "m": int64(1)}))
	require.False(t, chainVariablesBindEntities(matches, map[string]interface{}{"n": node, "r": node}))
}
