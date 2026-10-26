package replication

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestMultiRegionApplyPreservesOuterReadinessBeforeDelegation(t *testing.T) {
	for _, test := range []struct {
		name            string
		closed, started bool
		want            error
	}{{"not-started", false, false, ErrNotReady}, {"closed-before-started", true, false, ErrClosed}, {"closed-after-started", true, true, ErrClosed}} {
		t.Run(test.name, func(t *testing.T) {
			r := &MultiRegionReplicator{}
			r.closed.Store(test.closed)
			r.started.Store(test.started)
			require.ErrorIs(t, r.Apply(&Command{}, time.Second), test.want)
		})
	}
	local := &RaftReplicator{}
	local.closed.Store(true)
	r := &MultiRegionReplicator{localRaft: local}
	r.started.Store(true)
	require.ErrorIs(t, r.Apply(&Command{}, time.Second), ErrClosed, "ready outer mode delegates to actual local Raft")
}
