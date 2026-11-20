package replication

import (
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-288: a definite discard is decided inside Raft work that Shutdown joins,
// while the caller's settlement observer runs outside it. This test keeps the
// settlement observer blocked and establishes that Shutdown still completes and
// that the receipt stays undelivered until that observer returns. It fails on a
// waiter bounded by the delivered receipt (Done), or on a notification delivered
// from the deciding frame that Shutdown joins.
func TestRaftDiscardNotificationDoesNotBoundJoinedWork(t *testing.T) {
	fixture := newRaftDiscardSettlementLeader(t)
	t.Cleanup(func() {
		_ = fixture.raft.Shutdown()
		_ = fixture.adptr.Close()
		_ = fixture.base.Close()
	})
	settled := make(chan storage.CommitOutcome, 1)
	permit := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(permit) }) }
	t.Cleanup(release)
	receipt := fixture.submit(t, storage.NodeID("test:raft-discard-decision"), func(outcome storage.CommitOutcome) {
		settled <- outcome
		<-permit
	})
	fixture.appendUncommitted(t)
	fixture.conflict(t)
	requireDiscarded(t, requireRaftDiscardSettlementStep(t, "the discard outcome reaching its settlement observer", settled), ErrNotLeader)
	// The settlement observer is foreign code running in its own frame: no
	// accepted Raft work and no Shutdown may wait for it.
	shutdown := make(chan error, 1)
	go func() { shutdown <- fixture.raft.Shutdown() }()
	select {
	case err := <-shutdown:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("RaftReplicator.Shutdown waited for a settlement observer that has not returned")
	}
	select {
	case <-receipt.Done():
		t.Fatal("the receipt became terminal before its settlement observer returned")
	default:
	}
	release()
	select {
	case <-receipt.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the receipt never reached its terminal delivery")
	}
	require.Equal(t, storage.CommitDiscarded, receipt.Outcome().State)
	select {
	case duplicate := <-settled:
		t.Fatalf("the settlement observer ran twice; second outcome %+v", duplicate)
	case <-time.After(100 * time.Millisecond):
	}
}
