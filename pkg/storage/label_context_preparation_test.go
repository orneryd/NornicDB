package storage

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Cancel only at the owner's own observation, never in its earlier storage
// scan or a later visitor. after=2 reaches enumeration after entry admission.
type preparationCancelContext struct {
	context.Context
	owner           string
	additionalOwner string
	observed        string
	after, hits     int
	cancel          context.CancelFunc
}

func (c *preparationCancelContext) Err() error {
	var pcs [2]uintptr
	n := runtime.Callers(2, pcs[:])
	if n > 0 {
		frame, _ := runtime.CallersFrames(pcs[:n]).Next()
		if strings.HasSuffix(frame.Function, c.owner) || strings.Contains(frame.Function, "/pkg/storage."+c.owner+"[") || c.additionalOwner != "" && strings.HasSuffix(frame.Function, c.additionalOwner) {
			c.observed = frame.Function
			c.hits++
			if c.hits >= c.after {
				c.cancel()
			}
		}
	}
	return c.Context.Err()
}

func preparationContext(t *testing.T, owner string, after int) *preparationCancelContext {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &preparationCancelContext{Context: ctx, owner: owner, after: after, cancel: cancel}
}

func TestLabelContextConstraintPreparation(t *testing.T) {
	for _, owner := range []string{"keyCandidatesLockedWithContext", "trackWithContext", "pendingValueMatchesLockedWithContext"} {
		t.Run(owner, func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour)
			sm := b.GetSchemaForNamespace("test")
			require.NoError(t, sm.AddConstraint(Constraint{Name: "pair", Type: ConstraintNodeKey, Label: "Target", Properties: []string{"a", "b"}}))
			idx, owned := sm.ConstraintPropertyIndex("pair")
			require.True(t, owned)
			require.NoError(t, sm.BackfillCompositeIndex(idx.Name, nil))
			for i := 0; i < 20; i++ {
				bufferedCreate(t, b, &Node{ID: NodeID(fmt.Sprintf("test:b%d", i)), Labels: []string{"Target"}, Properties: map[string]interface{}{"a": int64(1), "b": int64(i)}})
			}
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			defer tx.Rollback()
			if owner == "pendingValueMatchesLockedWithContext" {
				for i := 0; i < 20; i++ {
					_, err = tx.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:p%d", i)), Labels: []string{"Other"}})
					require.NoError(t, err)
				}
			}
			// Tracking is normally warmed by the producer. Remove only the derived
			// pair so this public validation also exercises first-use preparation.
			if owner == "trackWithContext" {
				w := b.writeBehind
				w.mu.Lock()
				w.trackedPairs = nil
				delete(w.pendingIndex.tracked, "Target")
				w.mu.Unlock()
			}
			ctx := preparationContext(t, owner, 2)
			_, err = tx.CreateNodeWithContext(ctx, &Node{ID: "test:new", Labels: []string{"Target"}, Properties: map[string]interface{}{"a": int64(1), "b": int64(99)}})
			require.GreaterOrEqual(t, ctx.hits, 2, "cancellation must occur inside preparation")
			require.ErrorIs(t, err, context.Canceled)
			_, present := tx.pendingNodes["test:new"]
			require.False(t, present, "failed validation cannot publish a pending node")
			// An interrupted build must be retryable, with the same candidates.
			_, err = tx.CreateNode(&Node{ID: "test:retry", Labels: []string{"Target"}, Properties: map[string]interface{}{"a": int64(1), "b": int64(99)}})
			require.NoError(t, err)
			_, err = tx.CreateNode(&Node{ID: "test:duplicate", Labels: []string{"Target"}, Properties: map[string]interface{}{"a": int64(1), "b": int64(0)}})
			require.Error(t, err)
		})
	}
}

func TestLabelContextCommittedCandidateFiltering(t *testing.T) {
	b, cleanup := setupTestBadgerEngine(t)
	defer cleanup()
	for i := 0; i < 4; i++ {
		commitNodes(t, b, &Node{ID: NodeID(fmt.Sprintf("test:f%d", i)), Labels: []string{"Target"}, Properties: map[string]interface{}{"a": int64(i)}})
	}
	require.NoError(t, b.GetSchemaForNamespace("test").AddConstraint(Constraint{Name: "key", Type: ConstraintNodeKey, Label: "Target", Properties: []string{"a"}}))
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	ctx := preparationContext(t, "committedConstraintNodesLockedWithContext", 2)
	_, err = tx.CreateNodeWithContext(ctx, &Node{ID: "test:new", Labels: []string{"Target"}, Properties: map[string]interface{}{"a": int64(99)}})
	require.GreaterOrEqual(t, ctx.hits, 2)
	require.ErrorIs(t, err, context.Canceled)
}

func TestLabelContextEqualityPreparation(t *testing.T) {
	for _, owner := range []string{"LookupNodesByEqualityWithContext", "trackWithContext"} {
		t.Run(owner, func(t *testing.T) {
			b := createTestBadgerEngine(t)
			for i := 0; i < 12; i++ {
				_, err := b.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:eq%d", i)), Labels: []string{"Target"}, Properties: map[string]interface{}{"a": int64(1)}})
				require.NoError(t, err)
			}
			tx := beginMergeKeyTx(t, b)
			defer tx.Rollback()
			ctx := preparationContext(t, owner, 2)
			nodes, _, err := tx.LookupNodesByEqualityWithContext(ctx, "Target", map[string]interface{}{"a": int64(1)})
			require.GreaterOrEqual(t, ctx.hits, 2)
			require.ErrorIs(t, err, context.Canceled)
			require.Empty(t, nodes)
			nodes, complete, err := tx.LookupNodesByEquality("Target", map[string]interface{}{"a": int64(1)})
			require.NoError(t, err)
			require.True(t, complete)
			require.Len(t, nodes, 12, "interrupted preparation must be retryable")
		})
	}
}

type preparationLegacyLabelEngine struct{ Engine }

func TestLabelContextCollectorTail(t *testing.T) {
	t.Run("buffered body tail", func(t *testing.T) {
		b := newWriteBehindTestEngine(t, time.Hour)
		bufferedCreate(t, b, &Node{ID: "test:tail", Labels: []string{"Target"}})
		ctx := preparationContext(t, "StreamNodesWithOptions", 1)
		seen := 0
		err := b.StreamNodesWithOptions(ctx, StreamNodesOptions{}, func(*Node) error { seen++; return nil })
		require.GreaterOrEqual(t, ctx.hits, 1)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, seen)
	})
	t.Run("composite legacy collection", func(t *testing.T) {
		base := newTestMemoryEngine(t)
		_, err := base.CreateNode(&Node{ID: "test:tail", Labels: []string{"Target"}})
		require.NoError(t, err)
		c := NewCompositeEngine(map[string]Engine{"one": preparationLegacyLabelEngine{base}}, map[string]string{"one": "test"}, map[string]string{"one": "read"})
		ctx := preparationContext(t, "ForEachNodeIDByLabelWithContext", 2)
		seen := 0
		err = c.ForEachNodeIDByLabelWithContext(ctx, "Target", func(NodeID) bool { seen++; return true })
		require.GreaterOrEqual(t, ctx.hits, 2)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, seen)
	})
}

func TestLabelContextValidationCollections(t *testing.T) {
	b, cleanup := setupTestBadgerEngine(t)
	defer cleanup()
	for i := 0; i < 4; i++ {
		commitNodes(t, b, temporalFact(fmt.Sprintf("v%d", i), int64(i), 0, 1))
	}
	for _, tc := range []struct {
		kind  ConstraintType
		owner string
		props []string
	}{
		{ConstraintUnique, "validateUniqueConstraintOnCreationWithEngineWithContext", []string{"record_id"}},
		{ConstraintNodeKey, "validateNodeKeyConstraintOnCreationWithEngineWithContext", []string{"record_id"}},
		{ConstraintExists, "validateExistenceConstraintOnCreationWithEngineWithContext", []string{"record_id"}},
		{ConstraintDomain, "validateDomainConstraintOnCreationForEngineWithContext", []string{"record_id"}},
		{ConstraintTemporal, "validateTemporalConstraintOnCreationWithEngineWithContext", factWindow.Properties},
	} {
		t.Run(tc.owner, func(t *testing.T) {
			ctx := preparationContext(t, tc.owner, 2)
			err := ValidateConstraintOnCreationForEngineWithContext(ctx, b, Constraint{Name: "check", Type: tc.kind, Label: "Fact", Properties: tc.props, AllowedValues: []interface{}{int64(0), int64(1), int64(2), int64(3)}})
			require.GreaterOrEqual(t, ctx.hits, 2)
			require.ErrorIs(t, err, context.Canceled)
		})
	}
	t.Run("type", func(t *testing.T) {
		ctx := preparationContext(t, "ValidatePropertyTypeConstraintOnCreationForEngineWithContext", 2)
		err := ValidatePropertyTypeConstraintOnCreationForEngineWithContext(ctx, b, PropertyTypeConstraint{Label: "Fact", Property: "record_id", ExpectedType: "INTEGER"})
		require.GreaterOrEqual(t, ctx.hits, 2)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestLabelContextTemporalBackfillBody(t *testing.T) {
	b, cleanup := setupTestBadgerEngine(t)
	defer cleanup()
	commitNodes(t, b, temporalFact("old-a", int64(1), 0, 1), temporalFact("old-b", int64(2), 0, 1))
	sm := b.GetSchemaForNamespace("test")
	require.NoError(t, sm.AddConstraint(factWindow))
	ctx := preparationContext(t, "backfillTemporalIndexForLabelWithContext.func2", 2)
	err := MarkNewTemporalConstraintsForEngineWithContext(ctx, b, sm, []Constraint{factWindow})
	require.GreaterOrEqual(t, ctx.hits, 2)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, b.temporalIndexAuthoritative(sm, factWindow), "canceled body pass cannot certify completeness")
	require.NoError(t, MarkNewTemporalConstraintsForEngine(b, sm, []Constraint{factWindow}))
	require.True(t, b.temporalIndexAuthoritative(sm, factWindow))
}
