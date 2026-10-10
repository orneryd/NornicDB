package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Path selectors, quantified path patterns and their rewrites on their
// edges: rows and errors as Neo4j 2026.09 gives them, and the paths that
// stop on an error or a cancelled statement (#907).
func TestPatternEdges(t *testing.T) {
	exec, ctx := newPathSelectorExecutor(t)
	l := func(values ...interface{}) []interface{} { return values }
	for query, want := range map[string][][]interface{}{
		"MATCH ((x)-->(y))*(b:SP {id: 1}) RETURN count(*) AS c":                                           {l(int64(5))},
		"MATCH (b:SP {id: 2}) MATCH (a:SP {id: 1})((x)-->(y))+(b) RETURN count(*) AS c":                   {l(int64(2))},
		"MATCH p = ANY SHORTEST (:SP {id: 1})-->+(:SP {id: 5}) RETURN length(p) AS l":                     {l(int64(3))},
		"MATCH p = (:SP|X {id: 1}) RETURN length(p) AS l":                                                 {l(int64(0))},
		"MATCH p = ANY SHORTEST ((a:SP {id: 1})-->+(b) WHERE b:SP|X AND b.id = 5) RETURN length(p) AS l":  {l(int64(3))},
		"MATCH p = ANY SHORTEST ((a:SP {id: 1})-->+(b) WHERE b IS SP AND b.id = 5) RETURN length(p) AS l": {l(int64(3))},
		"RETURN __nornic_acyclic(1) AS a":                                                                 {l(nil)},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}
	for query, message := range map[string]string{
		"MATCH (a)-[r]->{2,1}(b) RETURN 1":                                                                        "must not have a lower bound which exceeds its upper bound",
		"MATCH p = ANY SHORTEST (a:SP)-->+(b:SP {id: 5}) WHERE a.id / 0 = 1 RETURN p":                             "by zero",
		"MATCH p = ANY SHORTEST (a:SP {id: 1})-->+(b:SP {id: 5}) WHERE length(p) / 0 = 1 RETURN p":                "by zero",
		"MATCH p = ANY SHORTEST (a:SP {id: 1})-->(m WHERE m.id / 0 = 1)-->+(b) RETURN p":                          "by zero",
		"MATCH (a:SP {id: 1})((x)-->(y))+(b) WHERE b.id / 0 = 1 RETURN 1":                                         "by zero",
		"MATCH (a:SP {id: 1})((x)-->(y) WHERE x.id / 0 = 1)+(b) RETURN 1":                                         "by zero",
		"MATCH p = ANY SHORTEST ((a)-->(b) WHERE a:A:B|C) RETURN p":                                               "Mixing label expression symbols",
		"MATCH ((x)-->(y) WHERE x:A:B|C)+ RETURN 1":                                                               "Mixing label expression symbols",
		"MATCH ((x:A:B|C)-->(y))+ RETURN 1":                                                                       "Mixing label expression symbols",
		"MATCH ((x)-->(y))*(b:SP {id: 1}) WHERE b.id / 0 = 1 RETURN 1":                                            "by zero",
		"RETURN __nornic_acyclic(1 / 0) AS a":                                                                     "by zero",
		"MATCH p = shortestPath((a)-->(b)), (c) WHERE __nornic_path_selector('ANY', 1, false, '', true) RETURN p": "Multiple path patterns cannot be used",
		"MATCH shortestPath((a)-->(b)) WHERE __nornic_path_selector('ANY', 1, false, '', true) RETURN 1":          "Multiple path patterns cannot be used",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), message, query)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	node := func(id int64) interface{} {
		result, err := exec.Execute(ctx, "MATCH (n:SP {id: $id}) RETURN n", map[string]interface{}{"id": id})
		require.NoError(t, err)
		return result.Rows[0][0]
	}
	rewritten, _, err := desugarLabelExpressions("MATCH p = SHORTEST 2 (a)-->+(b) RETURN p", nil)
	require.NoError(t, err)
	body := strings.TrimSuffix(strings.TrimPrefix(rewritten, "MATCH "), " RETURN p")
	m, ok, err := exec.parseShortestPathMatch(ctx, body)
	require.NoError(t, err, body)
	require.True(t, ok, body)
	for _, row := range []pipelineRow{{"a": node(1), "b": node(5)}, {"a": node(1), "b": node(1)}} {
		_, err = exec.pipelineApplyShortestPathMatch(cancelled, []pipelineRow{row}, m, false)
		require.ErrorIs(t, err, context.Canceled)
	}
	quantified, ok, err := parseQuantifiedPathMatch("(a:SP {id: 1})((x)-->(y))+(b)")
	require.NoError(t, err)
	require.True(t, ok)
	_, err = exec.pipelineApplyQuantifiedPathMatch(cancelled, []pipelineRow{{}}, quantified)
	require.ErrorIs(t, err, context.Canceled)

	for _, text := range []string{"(a WHERE a.s = '[*]')-->(b)", "(a)-[r WHERE r.w * 2 > 1]->(b)", "(a)-[r {w: 2}]->(b)"} {
		require.False(t, hasVariableLengthRelationship(text, 0, len(text)), text)
		require.False(t, hasUnboundedRepetition(text, 0, len(text)), text)
	}
	for text, unbounded := range map[string]bool{
		"(a)-[r*1..2 {w: 1}]->(b)":       false,
		"(a)-[r* {w: 1}]->(b)":           true,
		"(a)-[r*2.. {w: 1}]->(b)":        true,
		"(a)-[r WHERE r.w > 1]->{1,}(b)": true,
		"(a)((x)-->(y)){1,3}(b)":         false,
		"(a)((x)-->(y))+(b)":             true,
	} {
		require.Equal(t, unbounded, hasUnboundedRepetition(text, 0, len(text)), text)
	}
	// A pattern the clause pipeline can't run fails the step.
	_, err = exec.selectMatchedPaths(ctx, &shortestPathMatch{pathVariable: "p", pattern: "(a) (b)", selector: &pathSelector{}}, pipelineRow{}, 1)
	require.Error(t, err)
	err = exec.matchQuantifiedChain(ctx, "(a) (b)", "a", "", quantifiedPathState{used: map[storage.EdgeID]struct{}{}}, func(quantifiedPathState, pipelineRow) error { return nil })
	require.Error(t, err)
	require.False(t, parenthesisedPathAt("((a)", 0, 3))
	require.False(t, parenthesisedPathAt("(  )", 0, 3))
}
