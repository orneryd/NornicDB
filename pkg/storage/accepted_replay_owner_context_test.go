package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Author: NOR-288 lane (Claude), 2026-10-30. Accepted replay has no caller
// context: it takes the applying engine's work context, which only that
// engine's Close cancels. An open engine applies the replayed record durably;
// a closed engine leaves it pending with the closed store as its cause (not
// the cancellation of its own work context) and applies nothing.
func TestAcceptedReplayTakesApplyingEngineLifetime(t *testing.T) {
	t.Run("open", func(t *testing.T) {
		dir := t.TempDir()
		base, err := NewBadgerEngine(dir)
		require.NoError(t, err)
		p, payload := acceptedClosePhaseFrame(t, base, "test:open")
		p.Abort()
		data, err := marshalWALRecoveryPayload(nil, walEntityCommitData{Payload: payload})
		require.NoError(t, err)
		entry := WALEntry{Sequence: 1, Timestamp: time.Unix(1, 0), Operation: OpEntityCommit, Database: "test", Data: data, Checksum: crc32Checksum(data)}
		done := make(chan error, 1)
		go func() { done <- ReplayWALEntry(base, entry) }()
		require.NoError(t, requireWithin(t, "replay into an open engine", done))
		require.NoError(t, base.Close())
		reopened, err := NewBadgerEngine(dir)
		require.NoError(t, err)
		t.Cleanup(func() { _ = reopened.Close() })
		_, err = reopened.GetStoredNode("test:open")
		require.NoError(t, err, "the replayed record is durable")
	})
	t.Run("closed", func(t *testing.T) {
		dir := t.TempDir()
		base, err := NewBadgerEngine(dir)
		require.NoError(t, err)
		p, payload := acceptedClosePhaseFrame(t, base, "test:closed")
		logicalID := p.LogicalID()
		p.Abort()
		require.NoError(t, base.Close())
		done := make(chan error, 1)
		go func() { done <- ApplyAcceptedEntityTransaction(base, payload) }()
		err = requireWithin(t, "replay into a closed engine", done)
		var accepted *AcceptedCommitError
		require.ErrorAs(t, err, &accepted)
		require.Equal(t, CommitRecoveryPending, accepted.State)
		require.Equal(t, logicalID, accepted.LogicalID)
		require.ErrorIs(t, accepted.Cause(), ErrStorageClosed)
		require.False(t, errors.Is(accepted.Cause(), context.Canceled), "the closed store, not its cancelled work context, is the cause")
		reopened, err := NewBadgerEngine(dir)
		require.NoError(t, err)
		t.Cleanup(func() { _ = reopened.Close() })
		_, err = reopened.GetStoredNode("test:closed")
		require.True(t, errors.Is(err, ErrNotFound), "nothing was applied across the closed cut")
	})
}
