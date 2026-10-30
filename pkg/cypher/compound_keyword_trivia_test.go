package cypher

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCompoundKeywordTriviaSpans(t *testing.T) {
	for _, keyword := range []string{"OPTIONAL MATCH", "IS NOT NULL", "STARTS WITH", "ENDS WITH", "ON CREATE SET", "ON MATCH SET"} {
		for _, gap := range []string{" ", "\t", "/*c*/", " //c\n"} {
			text := strings.ReplaceAll(keyword, " ", gap)
			end, ok := keywordMatchAt(text+" tail", 0, keyword, 0, len(keyword))
			require.True(t, ok, text)
			require.Equal(t, len(text), end, text)
			require.Equal(t, len(text), keywordEndAt(text+" tail", 0, keyword), text)
		}
	}
	for _, text := range []string{"OPTIONALMATCH", "OPTIONAL/* unfinished", "OPTIONAL // MATCH"} {
		_, ok := keywordMatchAt(text, 0, "OPTIONAL MATCH", 0, len("OPTIONAL MATCH"))
		require.False(t, ok, text)
		require.False(t, startsWithKeywords(text, "OPTIONAL", "MATCH"), text)
		require.Equal(t, -1, findMultiWordKeywordIndex(text, "OPTIONAL", "MATCH"), text)
	}
	for _, text := range []string{"OPTIONAL/*c*/MATCH (n)", "OPTIONAL //c\nMATCH (n)"} {
		require.True(t, startsWithKeywords(text, "OPTIONAL", "MATCH"), text)
		require.Equal(t, "(n)", pipelineClauseBody(text, "OPTIONAL MATCH"), text)
		require.Equal(t, 0, findMultiWordKeywordIndex(text, "OPTIONAL", "MATCH"), text)
	}
	const text = "ON/*c*/CREATE //c\nSET n.x=1"
	allocs := testing.AllocsPerRun(100, func() {
		end, ok := keywordMatchAt(text, 0, "ON CREATE SET", 0, len("ON CREATE SET"))
		if !ok || end != len("ON/*c*/CREATE //c\nSET") {
			panic("compound keyword span")
		}
	})
	require.Zero(t, allocs)
}

func TestDistinctTriviaAdmission(t *testing.T) {
	for _, tc := range []struct {
		text, want string
		distinct   bool
	}{
		{"/*c*/ DISTINCT k", "k", true},
		{"//c\n DISTINCT k", "k", true},
		{"DISTINCT/*c*/k", "k", true},
		{"DISTINCT //c\nk", "k", true},
		{"/*c*/ distinctName", "distinctName", false},
		{"DISTINCT /*c*/ AS k", "DISTINCT /*c*/ AS k", false},
		{"DISTINCT /*c*/ .k", "DISTINCT /*c*/ .k", false},
		{"/* unfinished", "/* unfinished", false},
		{"DISTINCT /* unfinished", "DISTINCT /* unfinished", false},
	} {
		got, distinct := cutDistinct(tc.text)
		require.Equal(t, tc.want, got, tc.text)
		require.Equal(t, tc.distinct, distinct, tc.text)
	}
}

func TestCompoundKeywordOperatorCasePolicy(t *testing.T) {
	for _, text := range []string{" x STARTS WITH 'x'", " x STARTS  WITH 'x'", " x STARTS/*c*/WITH 'x'"} {
		require.True(t, operatorMatchesAt(text, " STARTS  WITH ", 2, false), text)
		left, right, ok := splitByOperatorWithOptions(text, " STARTS  WITH ", false, true)
		require.True(t, ok, text)
		require.Equal(t, "x", left, text)
		require.Equal(t, "'x'", right, text)
	}
	for _, tc := range []struct {
		text       string
		fold, want bool
	}{
		{" x STARTS/*c*/WITH 'x'", false, true},
		{" x STARTS //c\nWITH 'x'", false, true},
		{" x starts/*c*/WITH 'x'", false, false},
		{" x starts/*c*/with 'x'", true, true},
		{" x STARTS/* unfinished", false, false},
	} {
		got := operatorMatchesAt(tc.text, " STARTS WITH ", 2, tc.fold)
		require.Equal(t, tc.want, got, tc.text)
		if tc.want {
			left, right, ok := splitByOperatorWithOptions(tc.text, " STARTS WITH ", tc.fold, true)
			require.True(t, ok, tc.text)
			require.Equal(t, "x", left, tc.text)
			require.Equal(t, "'x'", right, tc.text)
		}
	}
}

func TestPipelineCompoundKeywordBodyBindings(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	for _, tc := range []struct{ clause, keyword string }{
		{"MATCH (n)-[r]->(m)", "MATCH"},
		{"CREATE (n)-[r]->(m)", "CREATE"},
		{"OPTIONAL MATCH (n)-[r]->(m)", "OPTIONAL MATCH"},
		{"OPTIONAL/*c*/MATCH (n)-[r]->(m)", "OPTIONAL MATCH"},
		{"OPTIONAL //c\nMATCH (n)-[r]->(m)", "OPTIONAL MATCH"},
	} {
		require.Equal(t, "(n)-[r]->(m)", pipelineClauseBody(tc.clause, tc.keyword), tc.clause)
		scope := make(map[string]struct{})
		addPipelinePatternBindings(t.Context(), exec, scope, tc.clause, tc.keyword)
		require.Equal(t, map[string]struct{}{"n": {}, "r": {}, "m": {}}, scope, tc.clause)
	}
}

func TestCorrelatedOptionalImportTrivia(t *testing.T) {
	restore := config.WithCypherQueryNormalizationDisabled()
	defer restore()
	for _, gap := range []string{" ", "/*c*/", " //c\n"} {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "optional_import_trivia")
		exec := NewStorageExecutor(store)
		_, err := store.CreateNode(&storage.Node{ID: "seed-1", Labels: []string{"Seed"}, Properties: map[string]interface{}{"name": "keep"}})
		require.NoError(t, err)
		_, err = store.CreateNode(&storage.Node{ID: "t-1", Labels: []string{"Target"}, Properties: map[string]interface{}{"name": "T1"}})
		require.NoError(t, err)
		require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e-1", Type: "REL", StartNode: "seed-1", EndNode: "t-1"}))
		outer := "MATCH (seed:Seed) WHERE seed.name = 'none' OPTIONAL" + gap + "MATCH (seed)-[:REL]->(t:Target) WITH seed, t"
		value, found, err := exec.resolveCorrelatedImportValue(t.Context(), outer, "seed", "seed-1", "t", map[string]map[string]interface{}{})
		require.NoError(t, err, gap)
		require.True(t, found, gap)
		switch target := value.(type) {
		case *storage.Node:
			require.Equal(t, storage.NodeID("t-1"), target.ID, gap)
		case map[string]interface{}:
			require.Equal(t, "t-1", target["_nodeId"], gap)
		default:
			t.Fatalf("unexpected import value type: %T", value)
		}
		query := "MATCH (seed:Seed) OPTIONAL" + gap + "MATCH (seed)-[:REL]->(t:Target) WITH seed, t CALL (t) { RETURN t.name AS j } RETURN j"
		result, err := exec.Execute(t.Context(), query, nil)
		require.NoError(t, err, gap)
		require.Equal(t, [][]interface{}{{"T1"}}, result.Rows, gap)
	}
}
