package storage

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

type residualDecisionContextKey struct{}

// These are invocation tests, not an implementation of #1615's arbiter.
// Vetoes run before acceptance; successful actions retain their actual outcome.
func TestResidualPublicationDecisionRoutes(t *testing.T) {
	for _, route := range []string{"ordinary", "large", "buffered", "prepared", "wal"} {
		for _, veto := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/veto=%v", route, veto), func(t *testing.T) {
				b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: route == "large", WriteBehind: route == "buffered", WriteBehindInterval: time.Hour})
				require.NoError(t, err)
				t.Cleanup(func() { _ = b.Close() })
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				t.Cleanup(func() { _ = tx.Rollback() })
				require.NoError(t, tx.SetNamespace("test"))
				require.NoError(t, tx.SetImplicit(route == "buffered"))
				if route == "large" {
					stageLargeCreate(t, tx, 6000)
				}
				_, err = tx.CreateNode(&Node{ID: "test:decision", Properties: map[string]any{"new-name": int64(1)}})
				require.NoError(t, err)
				var calls atomic.Int32
				var retained func() error
				rejected := errors.New("semantic owner veto")
				ctx, cancel := context.WithCancel(context.WithValue(context.Background(), residualDecisionContextKey{}, route))
				defer cancel()
				var callbackErr error
				ctx = WithPublicationDecision(ctx, func(actual context.Context, id PublicationIdentity, action func() error) error {
					calls.Add(1)
					if actual != ctx || id == (PublicationIdentity{}) {
						callbackErr = fmt.Errorf("decision lost its exact context or identity")
					}
					retained = action
					if veto {
						return rejected
					}
					outcome := action()
					if outcome != nil {
						callbackErr = outcome
					}
					// A callback error/cancellation after the authoritative action
					// cannot reinterpret its success as a rollback.
					cancel()
					return context.Canceled
				})
				if route == "prepared" || route == "wal" {
					prepared, prepErr := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: "decision-" + route})
					require.NoError(t, prepErr)
					t.Cleanup(prepared.Abort)
					require.Zero(t, calls.Load(), "preparation cannot decide acceptance")
					var owner EntityCommitSubmitter = b
					if route == "wal" {
						t.Cleanup(config.WithWALEnabled())
						dir := t.TempDir()
						wal, walErr := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "immediate"})
						require.NoError(t, walErr)
						t.Cleanup(func() { _ = wal.Close() })
						owner = NewWALEngine(b, wal)
					}
					receipt, submitErr := owner.SubmitEntityCommit(ctx, prepared, nil)
					err = submitErr
					if !veto && err == nil {
						require.NotNil(t, receipt)
						err = receipt.Wait(context.Background())
						require.Equal(t, CommitApplied, receipt.Outcome().State)
					}
					if veto {
						prepared.Abort()
					}
				} else {
					err = tx.CommitContext(ctx)
				}
				if veto {
					require.ErrorIs(t, err, rejected)
				} else {
					require.NoError(t, err)
				}
				require.NoError(t, callbackErr)
				require.EqualValues(t, 1, calls.Load())
				require.NotNil(t, retained)
				require.ErrorIs(t, retained(), ErrInvalidData, "returned/rejected actions cannot run later")
				require.NoError(t, b.FlushWriteBehind())
				_, readErr := b.GetStoredNode("test:decision")
				if veto {
					require.ErrorIs(t, readErr, ErrNotFound)
				} else {
					require.NoError(t, readErr)
				}
				require.EqualValues(t, 1, calls.Load(), "physical continuation/replay cannot decide again")
			})
		}
	}
}
