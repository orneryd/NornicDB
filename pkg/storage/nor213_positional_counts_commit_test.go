package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// NOR-213: stored relationship counts by (type, start label) and (type, end
// label) after transactions that rewrite a relationship (its type, start or
// end) at a node another transaction relabels, in either commit order.
// Rewrites have no Cypher form; they reach storage through UpdateEdge.

// nor213RequirePositionalCountsMatchGraph compares every stored count by type,
// start label and end label for the touched types and labels with a count
// over the stored relationships and their end nodes.
func nor213RequirePositionalCountsMatchGraph(t *testing.T, engine *BadgerEngine, what string) {
	t.Helper()
	ctx := t.Context()
	edges, err := engine.AllEdges(ctx)
	require.NoError(t, err)
	for _, edgeType := range []string{"LINK", "OTHER", "NEW"} {
		var total int64
		starts, ends := map[string]int64{}, map[string]int64{}
		for _, edge := range edges {
			if edge.Type != edgeType {
				continue
			}
			total++
			start, err := engine.GetNode(ctx, edge.StartNode)
			require.NoError(t, err)
			end, err := engine.GetNode(ctx, edge.EndNode)
			require.NoError(t, err)
			for _, label := range uniqueLabels(start.Labels) {
				starts[label]++
			}
			for _, label := range uniqueLabels(end.Labels) {
				ends[label]++
			}
		}
		stored, err := engine.EdgeCountByType(ctx, edgeType)
		require.NoError(t, err)
		require.Equal(t, total, stored, "%s: stored count of :%s", what, edgeType)
		for _, label := range []string{"Probe", "Held", "After", "Third"} {
			stored, err = engine.EdgeCountByStartLabel(ctx, label, edgeType)
			require.NoError(t, err)
			require.Equal(t, starts[label], stored, "%s: stored count of (:%s)-[:%s]->()", what, label, edgeType)
			stored, err = engine.EdgeCountByEndLabel(ctx, label, edgeType)
			require.NoError(t, err)
			require.Equal(t, ends[label], stored, "%s: stored count of ()-[:%s]->(:%s)", what, edgeType, label)
		}
	}
}

// requirePositionalChangesPending requires that tx holds a change from which
// its commit derives positional counts, so the commit takes the positional
// count lock (acquireCommitPublicationLocked).
func requirePositionalChangesPending(t *testing.T, tx *BadgerTransaction) {
	t.Helper()
	tx.mu.Lock()
	pending, err := tx.positionalChangesPendingLocked(t.Context())
	tx.mu.Unlock()
	require.NoError(t, err)
	require.True(t, pending, "the transaction must hold a positional count change")
}

// nor213PositionalCountEngine is a durable engine, or one with write-behind
// commits, holding x:Probe, y:Held and z:Held with relationships at both ends
// of y and z.
func nor213PositionalCountEngine(t *testing.T, writeBehind bool) *BadgerEngine {
	t.Helper()
	var engine *BadgerEngine
	if writeBehind {
		engine = newWriteBehindTestEngine(t, time.Hour)
	} else {
		engine = createTestBadgerEngine(t)
	}
	ctx := t.Context()
	for _, node := range []*Node{
		{ID: "test:x", Labels: []string{"Probe"}},
		{ID: "test:y", Labels: []string{"Held"}},
		{ID: "test:z", Labels: []string{"Held"}},
	} {
		_, err := engine.CreateNode(ctx, node)
		require.NoError(t, err)
	}
	for _, edge := range []*Edge{
		{ID: "test:link", StartNode: "test:x", EndNode: "test:y", Type: "LINK"},
		{ID: "test:own", StartNode: "test:x", EndNode: "test:y", Type: "OTHER"},
		{ID: "test:back", StartNode: "test:y", EndNode: "test:x", Type: "OTHER"},
		{ID: "test:zown", StartNode: "test:x", EndNode: "test:z", Type: "OTHER"},
		{ID: "test:zback", StartNode: "test:z", EndNode: "test:x", Type: "OTHER"},
	} {
		require.NoError(t, engine.CreateEdge(ctx, edge))
	}
	nor213RequirePositionalCountsMatchGraph(t, engine, "setup")
	return engine
}

// nor213Rewrites are relationship rewrites at y, the node the other
// transaction relabels.
var nor213Rewrites = []struct {
	name    string
	edge    EdgeID
	rewrite func(*Edge)
}{
	{"end-moved-onto-y", "test:zown", func(e *Edge) { e.EndNode = "test:y" }},
	{"end-moved-off-y", "test:own", func(e *Edge) { e.EndNode = "test:z" }},
	{"type-changed-at-y", "test:own", func(e *Edge) { e.Type = "NEW" }},
	{"start-moved-onto-y", "test:zback", func(e *Edge) { e.StartNode = "test:y" }},
	{"start-moved-off-y", "test:back", func(e *Edge) { e.StartNode = "test:z" }},
}

// TestTransactionRelabelAndRewriteCountsMatchTheStoredGraphInEitherCommitOrder:
// one transaction relabels y under its write lock and stays open while
// another rewrites a relationship at y; whichever commits first, the stored
// counts match the stored graph after each commit. With write-behind, the
// rewrite also commits as a buffered auto-commit while the relabel is open.
func TestTransactionRelabelAndRewriteCountsMatchTheStoredGraphInEitherCommitOrder(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		orders := []string{"relabel-last", "relabel-first"}
		if writeBehind {
			orders = append(orders, "buffered-rewrite-then-relabel")
		}
		for _, rewrite := range nor213Rewrites {
			for _, order := range orders {
				t.Run(fmt.Sprintf("writeBehind=%v/%s/%s", writeBehind, rewrite.name, order), func(t *testing.T) {
					engine := nor213PositionalCountEngine(t, writeBehind)
					ctx := t.Context()
					relabel, err := engine.BeginTransaction(ctx)
					require.NoError(t, err)
					defer relabel.Rollback()
					writer, err := engine.BeginTransaction(ctx)
					require.NoError(t, err)
					defer writer.Rollback()
					if order == "buffered-rewrite-then-relabel" {
						require.NoError(t, writer.SetImplicit(true))
						require.NoError(t, writer.SetNamespace("test"))
					}
					_, err = relabel.LockNodeForWrite(ctx, "test:y")
					require.NoError(t, err)
					require.NoError(t, relabel.UpdateGraphNode(ctx, NodeUpdateRequest{After: &Node{ID: "test:y", Labels: []string{"After"}}}))
					edge, err := writer.GetEdge(ctx, rewrite.edge)
					require.NoError(t, err)
					rewrite.rewrite(edge)
					require.NoError(t, writer.UpdateEdge(ctx, EdgeUpdateRequest{After: edge}))
					if order == "relabel-first" {
						require.NoError(t, relabel.Commit())
						nor213RequirePositionalCountsMatchGraph(t, engine, "after the relabel")
						require.NoError(t, writer.Commit())
					} else {
						require.NoError(t, writer.Commit())
						nor213RequirePositionalCountsMatchGraph(t, engine, "after the rewrite")
						require.NoError(t, relabel.Commit())
					}
					nor213RequirePositionalCountsMatchGraph(t, engine, "after both commits")
				})
			}
		}
	}
}

// TestTransactionRewriteOffANodeDeletedBeforeCommitIsRefused: a transaction
// moves a relationship away from y, and y with that relationship is deleted
// before it commits. The commit is refused as a conflict and the counts
// still match the stored graph.
func TestTransactionRewriteOffANodeDeletedBeforeCommitIsRefused(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		t.Run(fmt.Sprintf("writeBehind=%v", writeBehind), func(t *testing.T) {
			engine := nor213PositionalCountEngine(t, writeBehind)
			ctx := t.Context()
			tx, err := engine.BeginTransaction(ctx)
			require.NoError(t, err)
			defer tx.Rollback()
			edge, err := tx.GetEdge(ctx, "test:own")
			require.NoError(t, err)
			edge.EndNode = "test:z"
			require.NoError(t, tx.UpdateEdge(ctx, EdgeUpdateRequest{After: edge}))
			require.NoError(t, engine.DeleteNode(ctx, "test:y"))
			err = tx.Commit()
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound), "commit outcome: %v", err)
			nor213RequirePositionalCountsMatchGraph(t, engine, "after the refused commit")
		})
	}
}

// TestTransactionPositionalCountReadFallsBackForUnbalancedPositions: a
// transaction's own changes that leave every per-type and per-label count
// unchanged still move counts by type and end or start label, so a count
// read inside the transaction must not take the committed counter as its
// answer.
func TestTransactionPositionalCountReadFallsBackForUnbalancedPositions(t *testing.T) {
	cases := []struct {
		name    string
		stage   func(t *testing.T, ctx context.Context, tx *BadgerTransaction)
		request CountStoreReadRequest
		delta   int64
	}{
		{
			// OTHER from x moves its end from y:Held to z:After.
			name: "end-moved-between-labels",
			stage: func(t *testing.T, ctx context.Context, tx *BadgerTransaction) {
				edge, err := tx.GetEdge(ctx, "test:own")
				require.NoError(t, err)
				edge.EndNode = "test:z"
				require.NoError(t, tx.UpdateEdge(ctx, EdgeUpdateRequest{After: edge}))
			},
			request: CountStoreReadRequest{Kind: CountStoreRelationshipsByEndLabel, Namespace: "test", Name: "After", Types: []string{"OTHER"}},
			delta:   1,
		},
		{
			// y and z trade labels: the label counts are unchanged.
			name: "balanced-relabel",
			stage: func(t *testing.T, ctx context.Context, tx *BadgerTransaction) {
				require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: &Node{ID: "test:y", Labels: []string{"After"}}}))
				require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: &Node{ID: "test:z", Labels: []string{"Held"}}}))
			},
			request: CountStoreReadRequest{Kind: CountStoreRelationshipsByEndLabel, Namespace: "test", Name: "After", Types: []string{"LINK"}},
			delta:   1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			engine := nor213PositionalCountEngine(t, false)
			ctx := t.Context()
			require.NoError(t, engine.UpdateNode(ctx, NodeUpdateRequest{After: &Node{ID: "test:z", Labels: []string{"After"}}}))
			tx, err := engine.BeginTransaction(ctx)
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, tx.SetNamespace("test"))
			tc.stage(t, ctx, tx)
			answer, tracked, err := tx.PendingCountStoreDelta(ctx, tc.request)
			require.NoError(t, err)
			require.True(t, tracked)
			require.True(t, !answer.DescribesRows || answer.Count == tc.delta,
				"an in-transaction count must count rows or include the transaction's own change: %+v", answer)
			require.NoError(t, tx.Commit())
			nor213RequirePositionalCountsMatchGraph(t, engine, "after commit")
		})
	}
}
