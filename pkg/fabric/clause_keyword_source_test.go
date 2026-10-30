package fabric

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
)

// Author: #1663 implementation lane. The public planner must hand its keyword
// provider the actual nested view, including the original statement identity.
func TestPlannerClauseKeywordReaderRetainsOriginalSource(t *testing.T) {
	const prefix = "CYPHER 25 "
	const query = prefix + "CALL { USE alpha RETURN CASE WHEN true THEN {x: 1} ELSE {x: 2} END AS value } RETURN value"
	root := cyphertext.NewSource(context.Background(), query, cyphertext.AcceptedSource)
	input := root.Window(len(prefix), len(query))
	planner := NewFabricPlanner(nil)
	seen, nested := 0, false
	planner.SetClauseKeywordSourceNameReader(func(view *cyphertext.Source, start, end int, keyword string) bool {
		require.Same(t, root, view.OriginalView())
		require.Equal(t, keyword, view.Text[start:end])
		origin := view.Origin(cyphertext.Span{Start: start, End: end})
		if !origin.Generated {
			require.Equal(t, keyword, root.Text[origin.Original.Start:origin.Original.End])
		}
		seen++
		if len(view.Text) < len(input.Text) {
			nested = true
		}
		return false
	})
	_, err := planner.PlanSource(t.Context(), input, "nornic")
	require.NoError(t, err)
	require.Positive(t, seen)
	require.True(t, nested, "CALL-body readers must receive the carried child view")
	projected, err := planner.SingleGraphSource(t.Context(), input)
	require.NoError(t, err)
	require.Same(t, root, projected.OriginalView())

	// The existing exported callback remains an adapter to the same owner.
	legacy := NewFabricPlanner(nil)
	legacyCalls := 0
	legacy.SetClauseKeywordNameReader(func(text string, start, end int, keyword string) bool {
		require.Equal(t, keyword, text[start:end])
		legacyCalls++
		return false
	})
	// Planning reads the CALL body's clause keywords; single-graph projection
	// consults the reader only at top-level UNION candidates (release behaviour).
	_, err = legacy.Plan(t.Context(), input.Text, "nornic", AcceptedSource)
	require.NoError(t, err)
	require.Positive(t, legacyCalls)
	legacyText, err := legacy.SingleGraphStatement(t.Context(), input.Text, AcceptedSource)
	require.NoError(t, err)
	require.Equal(t, projected.Text, legacyText)
}
