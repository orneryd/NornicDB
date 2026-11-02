package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Commit-time constraint validation enumerates committed state through the
// canonical bulk readers (GetStoredNodesByLabel, GetStoredEdgesByType,
// GetStoredOutgoingEdges, GetStoredIncomingEdges). #1115 requires the caller
// context through commit preparation, so a terminated statement stops
// inside such an enumeration instead of after it.
//
// Each test cancels the commit context at the documented constraint-scan seam,
// which runs after the per-unit poll and immediately before the committed
// enumeration, while the commit holds its transaction mutex and the unique-value
// commit locks. The enumeration therefore starts on an already-cancelled
// context: the canonical reader must return that cancellation and no partial
// result, instead of materializing the committed candidate and reporting a
// constraint violation.

// TestCommitCancellationStopsCommittedNodeUniqueScan covers the node UNIQUE and
// NODE KEY committed fallback (scanForUniqueViolation / scanForNodeKeyViolation
// -> committedConstraintNodesLocked).
func TestCommitCancellationStopsCommittedNodeUniqueScan(t *testing.T) {
	engine := newIsolatedBadgerEngine(t)
	schema := engine.GetSchemaForNamespace("test")
	// A constraint whose value cache was never rebuilt validates through the
	// committed label scan.
	require.NoError(t, schema.AddUniqueConstraint("unique_k", "Item", "k"))

	staged, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = staged.Rollback() })
	stagedNode := &Node{ID: "test:staged", Labels: []string{"Item"}, Properties: map[string]any{"k": "shared"}}
	_, err = staged.CreateNode(t.Context(), stagedNode)
	require.NoError(t, err)

	// A peer claims the same constrained value after the staged write, so the
	// commit-time scan has a committed candidate to compare against.
	peer, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Rollback() })
	_, err = peer.CreateNode(t.Context(), &Node{ID: "test:peer", Labels: []string{"Item"}, Properties: map[string]any{"k": "shared"}})
	require.NoError(t, err)
	require.NoError(t, peer.Commit())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scans := 0
	restore := setUniqueConstraintScanHook(func() {
		scans++
		cancel() // the statement is terminated while its commit validates
	})
	defer restore()

	err = staged.CommitContext(ctx)
	require.NotZero(t, scans, "test setup: the commit did not take the committed label scan")
	require.ErrorIs(t, err, context.Canceled,
		"the committed node UNIQUE enumeration must observe the commit context, got %v", err)
	_, err = engine.GetNode(t.Context(), "test:staged")
	require.ErrorIs(t, err, ErrNotFound, "a cancelled commit stored its node")

	// The same write still commits with a live context once the client retries.
	retry, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = retry.Rollback() })
	_, err = retry.CreateNode(t.Context(), &Node{ID: "test:staged", Labels: []string{"Item"}, Properties: map[string]any{"k": "retried"}})
	require.NoError(t, err)
	require.NoError(t, retry.CommitContext(context.Background()))
	stored, err := engine.GetNode(t.Context(), "test:staged")
	require.NoError(t, err)
	require.Equal(t, "retried", stored.Properties["k"])
}

// TestCommitCancellationStopsCommittedNodeTemporalScan covers the node
// TEMPORAL committed scan (checkTemporalConstraint -> committedConstraintNodesLocked).
func TestCommitCancellationStopsCommittedNodeTemporalScan(t *testing.T) {
	engine := newIsolatedBadgerEngine(t)
	schema := engine.GetSchemaForNamespace("test")
	base := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, schema.AddConstraint(Constraint{
		Name:       "span_no_overlap",
		Type:       ConstraintTemporal,
		EntityType: ConstraintEntityNode,
		Label:      "Span",
		Properties: []string{"acct", "valid_from", "valid_to"},
	}))
	span := func(id, acct string, from, to time.Time) *Node {
		return &Node{ID: NodeID(id), Labels: []string{"Span"}, Properties: map[string]any{
			"acct": acct, "valid_from": from, "valid_to": to,
		}}
	}

	staged, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = staged.Rollback() })
	_, err = staged.CreateNode(t.Context(), span("test:span-staged", "a1", base.Add(time.Hour), base.Add(3*time.Hour)))
	require.NoError(t, err)

	// A peer opens an overlapping interval after the staged write.
	peer, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Rollback() })
	_, err = peer.CreateNode(t.Context(), span("test:span-peer", "a1", base, base.Add(2*time.Hour)))
	require.NoError(t, err)
	require.NoError(t, peer.Commit())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scans := 0
	restore := setTemporalConstraintScanHook(func() {
		scans++
		cancel()
	})
	defer restore()

	err = staged.CommitContext(ctx)
	require.NotZero(t, scans, "test setup: the commit did not take the committed temporal scan")
	require.ErrorIs(t, err, context.Canceled,
		"the committed node TEMPORAL enumeration must observe the commit context, got %v", err)
	_, err = engine.GetNode(t.Context(), "test:span-staged")
	require.ErrorIs(t, err, ErrNotFound, "a cancelled commit stored its node")

	// An unconstrained key does not overlap and commits with a live context.
	retry, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = retry.Rollback() })
	_, err = retry.CreateNode(t.Context(), span("test:span-staged", "a2", base.Add(time.Hour), base.Add(3*time.Hour)))
	require.NoError(t, err)
	require.NoError(t, retry.CommitContext(context.Background()))
	_, err = engine.GetNode(t.Context(), "test:span-staged")
	require.NoError(t, err)
}

// TestCommitCancellationStopsCommittedRelationshipUniqueScan covers the
// relationship UNIQUE committed scan (checkEdgeUniqueness ->
// GetStoredEdgesByType). The constrained value is a list, which the
// relationship equality index deliberately leaves to scan validation, so the
// check reads committed relationships instead of an index.
func TestCommitCancellationStopsCommittedRelationshipUniqueScan(t *testing.T) {
	engine := newIsolatedBadgerEngine(t)
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{
		Name:       "owns_unique_tags",
		Type:       ConstraintUnique,
		EntityType: ConstraintEntityRelationship,
		Label:      "OWNS",
		Properties: []string{"tags"},
	}))
	for _, id := range []NodeID{"test:a", "test:b", "test:c"} {
		_, err := engine.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Endpoint"}, Properties: map[string]any{}})
		require.NoError(t, err, "CreateNode(%q)", id)
	}
	owns := func(id, end string) *Edge {
		return &Edge{ID: EdgeID(id), StartNode: "test:a", EndNode: NodeID(end), Type: "OWNS",
			Properties: map[string]any{"tags": []any{"x"}}}
	}

	staged, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = staged.Rollback() })
	require.NoError(t, staged.CreateEdge(t.Context(), owns("test:owns-staged", "test:b")))

	// A peer claims the same constrained value after the staged write.
	peer, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Rollback() })
	require.NoError(t, peer.CreateEdge(t.Context(), owns("test:owns-peer", "test:c")))
	require.NoError(t, peer.Commit())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scans := 0
	restore := setUniqueConstraintScanHook(func() {
		scans++
		cancel()
	})
	defer restore()

	err = staged.CommitContext(ctx)
	require.NotZero(t, scans, "test setup: the commit did not take the committed edge scan")
	require.ErrorIs(t, err, context.Canceled,
		"the committed relationship UNIQUE enumeration must observe the commit context, got %v", err)
	_, err = engine.GetEdge(t.Context(), "test:owns-staged")
	require.ErrorIs(t, err, ErrNotFound, "a cancelled commit stored its relationship")

	// A different constrained value commits with a live context.
	retry, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = retry.Rollback() })
	require.NoError(t, retry.CreateEdge(t.Context(), &Edge{ID: "test:owns-staged", StartNode: "test:a", EndNode: "test:b", Type: "OWNS",
		Properties: map[string]any{"tags": []any{"y"}}}))
	require.NoError(t, retry.CommitContext(context.Background()))
	_, err = engine.GetEdge(t.Context(), "test:owns-staged")
	require.NoError(t, err)
}

// TestCommittedRelationshipConstraintReadsCarryTheCommitContext covers the
// remaining relationship families whose committed enumerations the commit
// validation performs: relationship TEMPORAL, CARDINALITY and the label-change
// POLICY check. No seam earlier in the same commit lets those reads start on a
// cancelled context without an earlier check returning first, so each is
// proven at the private helper that validateEdgeConstraints and
// validateAllConstraints call with the commit preparation context, a live
// context still reporting the established violation.
func TestCommittedRelationshipConstraintReadsCarryTheCommitContext(t *testing.T) {
	cancelledCtx := func() context.Context {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}
	day := func(month int) time.Time { return time.Date(2024, time.Month(month), 1, 0, 0, 0, 0, time.UTC) }

	t.Run("temporal", func(t *testing.T) {
		engine := newIsolatedBadgerEngine(t)
		c := Constraint{Name: "records_no_overlap", Type: ConstraintTemporal,
			EntityType: ConstraintEntityRelationship, Label: "RECORDS",
			Properties: []string{"subject", "valid_from", "valid_to"}}
		require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(c))
		for _, id := range []NodeID{"test:a", "test:b"} {
			_, err := engine.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Endpoint"}, Properties: map[string]any{}})
			require.NoError(t, err, "CreateNode(%q)", id)
		}
		interval := func(from, to int) map[string]any {
			return map[string]any{"subject": "s", "valid_from": day(from), "valid_to": day(to)}
		}
		require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b",
			Type: "RECORDS", Properties: interval(1, 6)}))

		tx, err := engine.BeginTransaction(t.Context())
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })

		probe := &Edge{ID: "test:e2", StartNode: "test:a", EndNode: "test:b", Type: "RECORDS", Properties: interval(2, 7)}
		var violation *ConstraintViolationError
		require.ErrorAs(t, tx.checkEdgeTemporalConstraint(context.Background(), probe, c, "test"), &violation,
			"a live context still reports the committed overlap")
		require.ErrorIs(t, tx.checkEdgeTemporalConstraint(cancelledCtx(), probe, c, "test"), context.Canceled)
	})

	t.Run("cardinality", func(t *testing.T) {
		engine := newIsolatedBadgerEngine(t)
		c := Constraint{Name: "owns_one_outgoing", Type: ConstraintCardinality,
			EntityType: ConstraintEntityRelationship, Label: "OWNS", Direction: "OUTGOING", MaxCount: 1}
		require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(c))
		for _, id := range []NodeID{"test:a", "test:b", "test:c"} {
			_, err := engine.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Endpoint"}, Properties: map[string]any{}})
			require.NoError(t, err, "CreateNode(%q)", id)
		}
		require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "OWNS"}))

		tx, err := engine.BeginTransaction(t.Context())
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })

		probe := &Edge{ID: "test:e2", StartNode: "test:a", EndNode: "test:c", Type: "OWNS"}
		var violation *ConstraintViolationError
		require.ErrorAs(t, tx.checkEdgeCardinality(context.Background(), probe, c, "test"), &violation,
			"a live context still reports the committed edge that fills the cardinality")
		require.ErrorIs(t, tx.checkEdgeCardinality(cancelledCtx(), probe, c, "test"), context.Canceled)
	})

	t.Run("label-change policy", func(t *testing.T) {
		engine := newIsolatedBadgerEngine(t)
		c := Constraint{Name: "no_secret_links", Type: ConstraintPolicy,
			EntityType: ConstraintEntityRelationship, Label: "LINK",
			PolicyMode: "DISALLOWED", SourceLabel: "User", TargetLabel: "Secret"}
		_, err := engine.CreateNode(t.Context(), &Node{ID: "test:u", Labels: []string{"User"}, Properties: map[string]any{}})
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &Node{ID: "test:s", Labels: []string{"Secret"}, Properties: map[string]any{}})
		require.NoError(t, err)
		// The relationship predates the policy: the check under test compares a label
		// change against committed relationships, not the policy's own admission.
		require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "test:e1", StartNode: "test:u", EndNode: "test:s", Type: "LINK"}))
		require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(c))

		tx, err := engine.BeginTransaction(t.Context())
		require.NoError(t, err)
		require.NoError(t, tx.SetNamespace("test"))
		t.Cleanup(func() { _ = tx.Rollback() })

		relabelled := &Node{ID: "test:u", Labels: []string{"User"}}
		before := &Node{ID: "test:u", Labels: []string{"Pending"}}
		var violation *ConstraintViolationError
		require.ErrorAs(t, tx.validatePolicyOnNodeLabelChange(context.Background(), relabelled, before), &violation,
			"a live context still reports the committed edge the policy disallows")
		require.ErrorIs(t, tx.validatePolicyOnNodeLabelChange(cancelledCtx(), relabelled, before), context.Canceled)
	})
}
