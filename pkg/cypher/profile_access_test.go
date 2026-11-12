package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	"github.com/orneryd/nornicdb/pkg/storage"

	"github.com/stretchr/testify/require"
)

func TestProfileAccessUsesAskingPredicateLifetime(t *testing.T) {
	old, current := newProfileRuntimeCollector(), newProfileRuntimeCollector()
	compiling := profileRuntimeClauseContext(withProfileRuntimeCollector(t.Context(), old), old, 0, pipelineClause{kind: pipelineClauseCall, text: "CALL previous"})
	asking := profileRuntimeClauseContext(withProfileRuntimeCollector(t.Context(), current), current, 0, pipelineClause{kind: pipelineClauseCall, text: "CALL current"})
	ctx := newRetainedPredicateContext(asking, compiling)
	queryprofile.Access(ctx, queryprofile.KindNodeByIDSeek, "node").Add(1, 1)
	require.Empty(t, old.accessObservation(), "a retained predicate cannot append to the statement that compiled it")
	observations := current.accessObservation()
	require.Len(t, observations, 1)
	require.Equal(t, int64(1), observations[0].Rows)
	require.Equal(t, int64(1), observations[0].Hits)
	frame, _ := profileRuntimeFrameFromContext(asking)
	require.Equal(t, frame.id, observations[0].Parent)
}

func TestProfileAccessAggregatesOnlyTheSameExecutingOwner(t *testing.T) {
	c := newProfileRuntimeCollector()
	ctx := withProfileRuntimeCollector(t.Context(), c)
	left := profileRuntimeClauseContext(ctx, c, 0, pipelineClause{kind: pipelineClauseMatch, text: "MATCH (a)"})
	right := profileRuntimeClauseContext(ctx, c, 0, pipelineClause{kind: pipelineClauseMatch, text: "MATCH (b)"})
	queryprofile.Access(left, queryprofile.KindNodeByLabelScan, "Person").Add(5, 5)
	queryprofile.Access(left, queryprofile.KindNodeByLabelScan, "Person").Add(2, 2)
	queryprofile.Access(right, queryprofile.KindNodeByLabelScan, "Person").Add(3, 3)
	got := c.accessObservation()
	require.Len(t, got, 2)
	require.Equal(t, int64(7), got[0].Rows)
	require.Equal(t, int64(7), got[0].Hits)
	require.Equal(t, int64(3), got[1].Rows)
	require.NotEqual(t, got[0].Parent, got[1].Parent)
	require.Zero(t, testing.AllocsPerRun(100, func() {
		queryprofile.Access(context.Background(), queryprofile.KindNodeByLabelScan, "Person").Add(1, 1)
	}))
}

func TestProfileNodeVisitorObservesReaderBeforeFiltering(t *testing.T) {
	visited := 0
	consume := func(*storage.Node) error { visited++; return nil }
	require.Zero(t, testing.AllocsPerRun(100, func() {
		visit := queryprofile.EntityVisitor(context.Background(), profileKindNodeByLabelScan, "Person", consume)
		_ = visit(nil)
	}))
	collector := newProfileRuntimeCollector()
	ctx := withProfileRuntimeCollector(t.Context(), collector)
	visit := queryprofile.EntityVisitor(ctx, profileKindNodeByLabelScan, "Person", consume)
	require.NoError(t, visit(nil))
	require.NoError(t, visit(&storage.Node{ID: "a"}))
	require.NoError(t, visit(&storage.Node{ID: "b"}))
	accesses := profileAccessesOfKind(collector, profileKindNodeByLabelScan)
	require.Len(t, accesses, 1)
	require.EqualValues(t, 2, accesses[0].Rows)
	require.EqualValues(t, 2, accesses[0].Hits)
	require.Greater(t, visited, 2, "the original consumer still receives nil and observed entries")
}

func TestProfileEntityPopulationSharesNonNilAndZeroAllocationRules(t *testing.T) {
	nodes := []*storage.Node{nil, {ID: "a"}, nil, {ID: "b"}}
	edges := []*storage.Edge{nil, {ID: "e"}, nil}
	require.Zero(t, testing.AllocsPerRun(100, func() {
		queryprofile.EntityPopulation(t.Context(), profileKindAllNodesScan, "", nodes)
		queryprofile.EntityPopulation(t.Context(), profileKindAllRelationshipsScan, "", edges)
	}))
	collector := newProfileRuntimeCollector()
	ctx := withProfileRuntimeCollector(t.Context(), collector)
	queryprofile.EntityPopulation(ctx, profileKindAllNodesScan, "", nodes)
	queryprofile.EntityPopulation(ctx, profileKindAllRelationshipsScan, "", edges)
	accesses := collector.accessObservation()
	require.Len(t, accesses, 2)
	require.EqualValues(t, 2, accesses[0].Rows)
	require.EqualValues(t, 2, accesses[0].Hits)
	require.EqualValues(t, 1, accesses[1].Rows)
	require.EqualValues(t, 1, accesses[1].Hits)
}
