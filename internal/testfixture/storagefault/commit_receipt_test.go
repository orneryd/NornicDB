package storagefault

import (
	"context"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// requireReceiptOutcome compares an outcome a receipt's waits return with the
// published one by state and cause: a wait presents an accepted (non-discarded)
// outcome as an AcceptedCommitError whose cause is reached through Cause().
func requireReceiptOutcome(t *testing.T, want, got storage.CommitOutcome) {
	t.Helper()
	require.Equal(t, want.State, got.State)
	if want.Err == nil || want.State == storage.CommitDiscarded {
		require.ErrorIs(t, got.Err, want.Err)
		return
	}
	var accepted *storage.AcceptedCommitError
	require.ErrorAs(t, got.Err, &accepted)
	require.ErrorIs(t, accepted.Cause(), want.Err)
}

func TestFinishPreparedReceipt(t *testing.T) {
	for _, test := range []struct {
		name           string
		state          storage.CommitOutcomeState
		alreadyAborted bool
	}{
		{"applied acknowledgement error", storage.CommitApplied, false},
		{"discarded", storage.CommitDiscarded, false},
		{"recovery pending", storage.CommitRecoveryPending, false},
		{"physical commit rejected", storage.CommitApplied, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true}))
			require.NoError(t, err)
			tx, err := base.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			require.NoError(t, tx.SetNamespace("test"))
			_, err = tx.CreateNode(&storage.Node{ID: "test:receipt"})
			require.NoError(t, err)
			prepared, err := tx.PrepareEntityCommit(t.Context(), storage.EntityCommitRequest{LogicalID: tx.ID})
			require.NoError(t, err)
			t.Cleanup(prepared.Abort)
			if test.alreadyAborted {
				prepared.Abort()
			}
			var settled []storage.CommitOutcome
			receipt, owner := storage.NewCommitReceipt(func(outcome storage.CommitOutcome) {
				settled = append(settled, outcome)
			})
			outcome := storage.CommitOutcome{State: test.state, Err: storage.ErrConflict}
			err = FinishPreparedReceipt(prepared, owner, outcome)
			if test.alreadyAborted {
				require.ErrorIs(t, err, storage.ErrTransactionClosed)
				require.Empty(t, settled)
				require.Equal(t, storage.CommitOutcome{State: storage.CommitRecoveryPending}, receipt.Outcome())
				select {
				case <-receipt.Done():
					t.Fatal("a failed physical commit must not resolve its receipt")
				default:
				}
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				var accepted *storage.AcceptedCommitError
				require.ErrorAs(t, receipt.Wait(ctx), &accepted)
				require.ErrorIs(t, accepted.Cause(), context.Canceled)
			} else {
				require.NoError(t, err)
				require.Equal(t, outcome, receipt.Outcome()) // Outcome and settle carry the published outcome itself
				requireReceiptOutcome(t, outcome, receipt.WaitPhysicalOutcome(t.Context()))
				if test.state == storage.CommitRecoveryPending {
					require.Empty(t, settled)
					requireReceiptOutcome(t, outcome, receipt.WaitOutcome(t.Context()))
				} else {
					require.Equal(t, []storage.CommitOutcome{outcome}, settled)
					requireReceiptOutcome(t, outcome, storage.CommitOutcome{State: outcome.State, Err: receipt.Wait(t.Context())})
				}
			}
			node, readErr := base.GetStoredNode("test:receipt")
			if test.state == storage.CommitApplied && !test.alreadyAborted {
				require.NoError(t, readErr)
				require.Equal(t, storage.NodeID("test:receipt"), node.ID)
			} else {
				require.ErrorIs(t, readErr, storage.ErrNotFound)
				require.Nil(t, node)
			}
		})
	}
}
