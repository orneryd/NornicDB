package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// These use real native spill/compensation, not a synthetic batch target. A
// canceled/rejected final operation must restore already-written preparation
// batches without consulting its canceled request or publication decision.
func TestLargeCommitRepairOwnsCompletionAfterRequestCancellation(t *testing.T) {
	for _, at := range []string{"written-batch", "publication-decision"} {
		t.Run(at, func(t *testing.T) {
			engine := openLargeCommitTestEngine(t, t.TempDir())
			_, err := engine.CreateNode(t.Context(), &Node{ID: "test:repair-base", Labels: []string{"Base"}, Properties: map[string]any{"v": int64(1)}})
			require.NoError(t, err)
			tx, err := engine.BeginTransaction(t.Context())
			require.NoError(t, err)
			t.Cleanup(func() {
				err := tx.Rollback()
				if err != nil {
					require.ErrorIs(t, err, ErrTransactionClosed)
				}
			})
			require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: &Node{ID: "test:repair-base", Labels: []string{"Base"}, Properties: map[string]any{"v": int64(2)}}, Policy: ClientNodeEdits}))
			stageLargeCreate(t, tx, 6000)
			request, cancel := context.WithCancel(context.Background())
			defer cancel()
			injected := errors.New("reject original publication")
			decisions, batches := 0, 0
			ctx := WithPublicationDecision(request, func(_ context.Context, _ PublicationIdentity, action func() error) error {
				decisions++
				if at == "publication-decision" {
					cancel()
					return injected
				}
				return action()
			})
			restore := setLargeCommitBatchHook(func(n int) error {
				batches = n
				if at == "written-batch" && n == 2 {
					cancel()
					return injected
				}
				return nil
			})
			t.Cleanup(restore)
			err = tx.CommitContext(ctx)
			restore()
			require.ErrorIs(t, err, injected)
			require.ErrorIs(t, request.Err(), context.Canceled)
			require.GreaterOrEqual(t, batches, 2, "actual physical preparation batches must have been written")
			want := 0
			if at == "publication-decision" {
				want = 1
			}
			require.Equal(t, want, decisions, "compensation must not re-enter original semantic decision")
			require.Zero(t, countBigNodes(t, engine))
			base, err := engine.GetNode(t.Context(), "test:repair-base")
			require.NoError(t, err)
			require.Equal(t, int64(1), base.Properties["v"])
			edges, err := engine.GetOutgoingEdges(context.Background(), largeCommitNodeID(0))
			require.NoError(t, err)
			require.Empty(t, edges)
			requireNoLargeCommitIntent(t, engine)
			require.NoError(t, engine.db.oracle.failure())
			_, err = engine.CreateNode(t.Context(), &Node{ID: "test:after-repair", Labels: []string{"Base"}})
			require.NoError(t, err, "compensation releases the native write gate")
		})
	}
}
