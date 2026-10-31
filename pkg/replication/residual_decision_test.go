package replication

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestResidualReplicatedPublicationDecision(t *testing.T) {
	for _, veto := range []bool{false, true} {
		t.Run(fmt.Sprintf("veto=%v", veto), func(t *testing.T) {
			base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
			require.NoError(t, err)
			t.Cleanup(func() { _ = base.Close() })
			r, mirror := newAcceptedReplicationFixture(t, base, nil, &Config{})
			owner := NewReplicatedEngine(base, r, time.Second)
			tx, err := base.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			_, err = tx.CreateNode(&storage.Node{ID: "test:replicated-decision", Properties: map[string]any{"v": int64(1)}})
			require.NoError(t, err)
			var calls atomic.Int32
			var retained func() error
			var callbackErr error
			rejected := errors.New("replication veto")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx = storage.WithPublicationDecision(ctx, func(actual context.Context, _ storage.PublicationIdentity, action func() error) error {
				calls.Add(1)
				if actual != ctx || mirror.calls.Load() != 0 {
					callbackErr = errors.New("decision lost context or ran after replication")
				}
				retained = action
				if veto {
					return rejected
				}
				if err := action(); err != nil {
					callbackErr = err
				}
				cancel()
				return context.Canceled
			})
			prepared, err := tx.PrepareEntityCommit(ctx, storage.EntityCommitRequest{LogicalID: "replication-decision"})
			require.NoError(t, err)
			t.Cleanup(prepared.Abort)
			require.Zero(t, calls.Load())
			receipt, err := owner.SubmitEntityCommit(ctx, prepared, nil)
			if veto {
				require.ErrorIs(t, err, rejected)
				require.Nil(t, receipt)
				prepared.Abort()
				require.Zero(t, mirror.calls.Load())
			} else {
				require.NoError(t, err)
				require.NoError(t, receipt.Wait(context.Background()))
				require.Equal(t, storage.CommitApplied, receipt.Outcome().State)
				require.EqualValues(t, 1, mirror.calls.Load())
			}
			require.NoError(t, callbackErr)
			require.EqualValues(t, 1, calls.Load())
			require.ErrorIs(t, retained(), storage.ErrInvalidData)
			_, err = base.GetStoredNode("test:replicated-decision")
			if veto {
				require.ErrorIs(t, err, storage.ErrNotFound)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
