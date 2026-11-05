package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRelationshipUniquePreparationCancellationColdRetryWarm(t *testing.T) {
	// The fixture commits the real peer before installing its scalar constraint.
	// No earlier check can have initialized the derived equality cache.
	engine := relationshipUniqueCommitFixture(t, 1)
	require.Empty(t, engine.relationshipUniqueIndexes.entries)
	staged, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = staged.Rollback() })
	require.NoError(t, staged.SetValidationSetting(DeferredConstraintValidation, true))
	edge := &Edge{ID: "test:cancelled", StartNode: "test:a", EndNode: "test:b", Type: "T", Properties: map[string]interface{}{"id": int64(0)}}
	require.NoError(t, staged.CreateEdge(t.Context(), edge))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scans := 0
	restore := setUniqueConstraintScanHook(func() { scans++; cancel() })
	err = staged.CommitContext(ctx)
	restore()
	require.Equal(t, 1, scans, "first derived cache build must reach the real scan boundary")
	require.ErrorIs(t, err, context.Canceled, "canceled preparation must not materialize the peer and report its constraint violation")
	require.Equal(t, TxStatusRolledBack, staged.Status)
	_, err = engine.GetStoredEdge(t.Context(), "test:cancelled")
	require.ErrorIs(t, err, ErrNotFound)
	require.Empty(t, engine.relationshipUniqueIndexes.entries, "cancellation must not publish a reusable partial cache")
	require.False(t, engine.relationshipUniqueIndexes.active.Load())

	retry, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, retry.SetValidationSetting(DeferredConstraintValidation, true))
	require.NoError(t, retry.CreateEdge(t.Context(), &Edge{ID: "test:healthy", StartNode: "test:a", EndNode: "test:b", Type: "T", Properties: map[string]interface{}{"id": int64(7)}}))
	require.NoError(t, retry.CommitContext(context.Background()))
	_, err = engine.GetStoredEdge(t.Context(), "test:healthy")
	require.NoError(t, err)
	require.Len(t, engine.relationshipUniqueIndexes.entries, 1)

	// A warm cache wait is preparation too. Cancel at the actual blocked lock
	// boundary, before it can read cached candidates or report a violation.
	blocked, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, blocked.SetValidationSetting(DeferredConstraintValidation, true))
	edge.ID = "test:warm-cancelled"
	require.NoError(t, blocked.CreateEdge(t.Context(), edge))
	lockCtx, lockCancel := context.WithCancel(context.Background())
	defer lockCancel()
	set := &engine.relationshipUniqueIndexes
	set.mu.Lock()
	waited := false
	restoreWait := setAdmissionWaitObserver(func(owner any, _ context.Context) {
		if owner == &set.mu {
			waited = true
			lockCancel()
		}
	})
	err = blocked.CommitContext(lockCtx)
	restoreWait()
	set.mu.Unlock()
	require.True(t, waited)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, TxStatusRolledBack, blocked.Status)
	_, err = engine.GetStoredEdge(t.Context(), edge.ID)
	require.ErrorIs(t, err, ErrNotFound)
	require.Len(t, set.entries, 1, "canceled warm lookup must preserve its complete cache")
	cancelled, cancelLookup := context.WithCancel(context.Background())
	cancelLookup()
	ids, indexed, err := engine.relationshipUniqueCandidates(cancelled, edge, Constraint{Properties: []string{"id"}}, "test")
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, ids)
	require.False(t, indexed)

	// A healthy warm-cache duplicate remains a real semantic violation.
	warm, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, warm.SetValidationSetting(DeferredConstraintValidation, true))
	edge.ID = "test:warm-duplicate"
	require.NoError(t, warm.CreateEdge(t.Context(), edge))
	var violation *ConstraintViolationError
	require.ErrorAs(t, warm.CommitContext(context.Background()), &violation)
	_, err = engine.GetStoredEdge(t.Context(), edge.ID)
	require.ErrorIs(t, err, ErrNotFound)
}

// The population and read are identical before/after the private context fix.
// Cold samples rebuild from stored edges; warm samples only seek cached values.
func BenchmarkRelationshipUniquePreparationContext(b *testing.B) {
	for _, existing := range []int{128, 512} {
		for _, cold := range []bool{true, false} {
			name := "warm"
			if cold {
				name = "cold"
			}
			b.Run(fmt.Sprintf("%s/edges%d", name, existing), func(b *testing.B) {
				engine := relationshipUniqueCommitFixture(b, existing)
				edge := &Edge{ID: "test:candidate", Type: "T", Properties: map[string]interface{}{"id": int64(0)}}
				constraint := Constraint{Type: ConstraintUnique, EntityType: ConstraintEntityRelationship, Label: "T", Properties: []string{"id"}}
				_, _, err := engine.relationshipUniqueCandidates(context.Background(), edge, constraint, "test")
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if cold {
						b.StopTimer()
						engine.resetRelationshipUniqueIndexes()
						b.StartTimer()
					}
					ids, indexed, err := engine.relationshipUniqueCandidates(context.Background(), edge, constraint, "test")
					if err != nil || !indexed || len(ids) != 1 {
						b.Fatalf("ids=%v indexed=%v err=%v", ids, indexed, err)
					}
				}
			})
		}
	}
}
