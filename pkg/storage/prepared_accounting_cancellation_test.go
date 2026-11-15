package storage

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestPreparedBufferedAccountingUsesAcceptedLifetime(t *testing.T) {
	preparation, cancel := context.WithCancel(t.Context())
	defer cancel()
	policy := preparedMemoryPolicy(t)
	b, w := preparedMemoryEngine(t, t.TempDir(), policy)
	require.NoError(t, b.InitializeStoredEntityObserver("test", func(StoredEntitySnapshot) (StoredEntityObserver, error) {
		return func(event StoredEntityPublication) (func(), error) {
			cancel()
			return func() {}, nil
		}, nil
	}, nil))
	tx, err := BeginEntityTransaction(preparation, b, CommitModeImplicit)
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	preparedMemoryStage(t, tx, 1, 1024)
	p, err := tx.PrepareEntityCommit(preparation, EntityCommitRequest{LogicalID: "accepted-accounting-cancel"})
	require.NoError(t, err)
	defer p.Abort()
	receipt, err := NewWALEngine(b, w).SubmitEntityCommit(t.Context(), p, nil)
	require.NoError(t, err)
	require.NotNil(t, receipt)
	wait, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	outcome := receipt.WaitOutcome(wait)
	require.NoError(t, outcome.Err, "original preparation cancellation cannot retract an accepted commit")
	require.ErrorIs(t, preparation.Err(), context.Canceled)
	require.Equal(t, 1, preparedMemoryStored(t, b))
	require.Zero(t, policy.Reserved())
}
