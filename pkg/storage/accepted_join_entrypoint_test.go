package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAcceptedJoinEntrypointsShareOneJoinOwner exercises both exported inline
// join entrypoints against the one private record/join owner every transport
// uses.
//
// The released three-argument entrypoint is the Go API external transports
// call; the release-aware entrypoint is that same join for a caller whose
// inline join runs inside its own accepted work enrolment. Here the command was
// accepted but no outer log recorded it, so a transport error is a
// recovery-pending observation in both cases: the frame is parked for recovery,
// the outer receipt is never settled, and the caller's release (when it has
// one) runs exactly once inside the join.
func TestAcceptedJoinEntrypointsShareOneJoinOwner(t *testing.T) {
	join := func(t *testing.T, nodeID string, releaseAware bool) {
		t.Helper()
		_, h, _ := acceptedRejectFixture(t, NodeID(nodeID))
		settled, releases := 0, 0
		receipt, owner := NewCommitReceipt(func(CommitOutcome) { settled++ })
		if releaseAware {
			h.JoinContextWithRelease(context.Background(), owner, ErrStorageClosed, func() { releases++ })
		} else {
			h.JoinContext(context.Background(), owner, ErrStorageClosed)
		}
		require.False(t, h.NativeFrameRetained(), "the join parks an unrecorded command's frame for recovery")
		select {
		case <-receipt.Done():
			t.Fatal("a recovery-pending observation must not settle the outer receipt")
		default:
		}
		outcome, _ := receipt.Decision()
		require.Equal(t, CommitRecoveryPending, outcome.State)
		var accepted *AcceptedCommitError
		require.ErrorAs(t, outcome.Err, &accepted)
		require.ErrorIs(t, accepted.Err, ErrStorageClosed)
		require.Zero(t, settled, "the outer settlement observer has no terminal outcome to receive")
		if releaseAware {
			require.Equal(t, 1, releases, "the join runs the caller's enrolment release exactly once")
		} else {
			require.Zero(t, releases)
		}
	}
	t.Run("released", func(t *testing.T) { join(t, "test:join-entrypoint-released", false) })
	t.Run("release-aware", func(t *testing.T) { join(t, "test:join-entrypoint-release", true) })
}
