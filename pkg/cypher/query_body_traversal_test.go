package cypher

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// Structural rules must see expression bodies even when the surrounding
// statement cannot resolve its names. Quoted text, comments and maps are
// not query bodies, and every reported span addresses the original source.
func TestReadQueryBodyExpressionWindows(t *testing.T) {
	type body struct{ container, text string }
	for _, tc := range []struct {
		name, source string
		want         []body
		firstKinds   []pipelineClauseKind
	}{
		{
			name: "query bodies with quoted and comment decoys",
			source: "/* CALL { RETURN 0 } */ WITH 'COUNT { RETURN 0 }' AS text, `EXISTS { decoy }` AS quoted " +
				"CALL ( missing ) { RETURN COLLECT { WITH missing RETURN missing } AS values } " +
				"RETURN EXISTS { MATCH (n) RETURN n } AS present, COUNT { MATCH (m) RETURN m } AS total, {key: 1} AS map // COLLECT { RETURN 0 }",
			want: []body{{"CALL", " RETURN COLLECT { WITH missing RETURN missing } AS values "}, {"COLLECT", " WITH missing RETURN missing "}, {"EXISTS", " MATCH (n) RETURN n "}, {"COUNT", " MATCH (m) RETURN m "}},
		},
		{
			name:       "pattern and query expression bodies",
			source:     "WITH missing AS outerValue RETURN EXISTS { (a)-[:R]->(b) WHERE a.id = b.id } AS present, COUNT { MATCH (c) } AS total, COLLECT { MATCH (d) RETURN d } AS values",
			want:       []body{{"EXISTS", " (a)-[:R]->(b) WHERE a.id = b.id "}, {"COUNT", " MATCH (c) "}, {"COLLECT", " MATCH (d) RETURN d "}},
			firstKinds: []pipelineClauseKind{pipelineClauseMatch, pipelineClauseMatch, pipelineClauseMatch},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []body
			var kinds []pipelineClauseKind
			require.NoError(t, forEachQueryBody(t.Context(), originalBraceView(tc.source), queryBodyContainerKeywords[:], func(container string, bodyView braceView) error {
				start, end := testBodyWindow(bodyView)
				require.Greater(t, start, 0)
				require.Less(t, end, len(tc.source))
				require.Equal(t, byte('{'), tc.source[start-1])
				require.Equal(t, byte('}'), tc.source[end])
				got = append(got, body{container, tc.source[start:end]})
				if tc.firstKinds != nil {
					shape := readQueryBody(t.Context(), originalBraceView(tc.source), start, end)
					require.Equal(t, queryBodyRead, shape.status, tc.source[start:end])
					require.Len(t, shape.branches, 1)
					require.NotEmpty(t, shape.branches[0].clauses)
					kinds = append(kinds, shape.branches[0].clauses[0].kind)
				}
				return nil
			}))
			require.Equal(t, tc.want, got)
			if tc.firstKinds != nil {
				require.Equal(t, tc.firstKinds, kinds)
			}
			var wantSelected, selected []body
			for _, item := range tc.want {
				if item.container == "COLLECT" {
					wantSelected = append(wantSelected, item)
				}
			}
			require.NoError(t, forEachQueryBody(t.Context(), originalBraceView(tc.source), []string{"COLLECT"}, func(container string, bodyView braceView) error {
				start, end := testBodyWindow(bodyView)
				selected = append(selected, body{container, tc.source[start:end]})
				return nil
			}))
			require.Equal(t, wantSelected, selected)
			stop := errors.New("stop structural validation")
			visits := 0
			err := forEachQueryBody(t.Context(), originalBraceView(tc.source), queryBodyContainerKeywords[:], func(string, braceView) error { visits++; return stop })
			require.ErrorIs(t, err, stop)
			require.Equal(t, 1, visits, "a rule error must stop the traversal")
		})
	}
	require.NoError(t, forEachQueryBody(t.Context(), originalBraceView("RETURN 1"), queryBodyContainerKeywords[:], func(string, braceView) error {
		t.Fatal("a statement without braces has no query bodies")
		return nil
	}))
}
