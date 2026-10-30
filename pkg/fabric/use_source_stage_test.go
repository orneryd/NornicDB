package fabric

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestUseRawSourceUnicodeSpans(t *testing.T) {
	query := `  \u0055SE graph.byName('\u0061') RETURN '\u0062' AS x`
	reference, rest, found, err := ReadUseClause(t.Context(), query, false, cyphertext.RawSource)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "graph.byName", reference.Function)
	require.Equal(t, []string{`'\u0061'`}, reference.Args)
	require.Equal(t, `RETURN '\u0062' AS x`, rest)
	require.Equal(t, `  \u0055SE graph.byName('\u0061')`, query[:reference.End])
	require.Equal(t, rest, query[reference.Remainder.Start:reference.Remainder.End])
}

func TestUseAcceptedSourceDoesNotProjectAgain(t *testing.T) {
	query := `USE graph.byName('\u0061') RETURN '\u0062' AS x`
	ref, rest, found, err := ReadUseClause(t.Context(), query, false, cyphertext.AcceptedSource)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []string{`'\u0061'`}, ref.Args)
	require.Equal(t, `RETURN '\u0062' AS x`, rest)
	require.Equal(t, len(`USE graph.byName('\u0061')`), ref.End)
	require.Equal(t, rest, query[ref.Remainder.Start:ref.Remainder.End])
	use, _, found, err := ParseUseClause(t.Context(), "USE `\\u0061` RETURN 1", false, cyphertext.AcceptedSource)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, `\u0061`, use.Name)
}

func TestUseSourceCancellationAndPlannerIngress(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, stage := range []cyphertext.SourceStage{cyphertext.RawSource, cyphertext.AcceptedSource} {
		_, _, _, err := ReadUseClause(ctx, "RETURN 1", false, stage)
		require.ErrorIs(t, err, context.Canceled)
		_, _, _, err = ParseUseClause(ctx, "USE cmp.a RETURN 1", false, stage)
		require.ErrorIs(t, err, context.Canceled)
	}
	planner := NewFabricPlanner(plannerUseCatalog())
	for _, stage := range []SourceStage{RawSource, AcceptedSource} {
		_, err := planner.Plan(ctx, "", "cmp", stage)
		require.ErrorIs(t, err, context.Canceled)
		_, err = planner.SingleGraphStatement(ctx, "", stage)
		require.ErrorIs(t, err, context.Canceled)
	}
	fragment, err := planner.Plan(t.Context(), `\u0055SE cmp.\u0061 RETURN '\u005cu0062' AS x`, "cmp", cyphertext.RawSource)
	require.NoError(t, err)
	execution, ok := fragment.(*FragmentExec)
	require.True(t, ok)
	require.Equal(t, "cmp.a", execution.GraphName)
	require.Equal(t, `RETURN '\u0062' AS x`, execution.Query)
	_, err = planner.Plan(ctx, "USE cmp.a RETURN 1", "cmp", cyphertext.RawSource)
	require.ErrorIs(t, err, context.Canceled)
	_, err = planner.SingleGraphStatement(ctx, "USE cmp.a RETURN 1", cyphertext.AcceptedSource)
	require.ErrorIs(t, err, context.Canceled)
}
