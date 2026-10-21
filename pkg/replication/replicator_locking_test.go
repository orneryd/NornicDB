package replication

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// newLeaderWithPeer returns a started leader at term 1 with one connected peer.
func newLeaderWithPeer(t *testing.T) *RaftReplicator {
	t.Helper()
	r, _ := newCoverageRaftReplicator(t)
	r.started.Store(true)
	r.state = StateLeader
	r.currentTerm = 1
	r.config.Raft.Peers = []PeerConfig{{ID: "p1", Addr: "127.0.0.1:17201"}}
	r.peerConns["p1"] = &raftCoveragePeerConn{connected: true}
	r.nextIndex["p1"] = 1
	r.matchIndex["p1"] = 0
	return r
}

// The leader builds AppendEntries from nextIndex while responses from the same
// peer rewrite it. nextIndex belongs to peerMu, so both sides must hold it.
func TestRaftReplicator_ReplicateLogToPeerReadsNextIndexUnderPeerMu(t *testing.T) {
	r := newLeaderWithPeer(t)
	peer := r.config.Raft.Peers[0]
	resp := &AppendEntriesResponse{Term: 1, Success: false, ConflictIndex: 1}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			r.replicateLogToPeer(context.Background(), peer, 1)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			r.handleAppendEntriesResponse(peer.ID, 1, &AppendEntriesRequest{Term: 1}, resp)
		}
	}()
	wg.Wait()
}

// Membership changes rewrite config.Raft.Peers under peerMu while elections,
// applies and heartbeats read it.
func TestRaftReplicator_PeerListReadsHoldPeerMu(t *testing.T) {
	r := newLeaderWithPeer(t)
	r.config.Raft.CommitTimeout = time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			id := fmt.Sprintf("v%d", i)
			if err := r.AddVoter(id, "127.0.0.1:17202"); err != nil {
				t.Error(err)
				return
			}
			if err := r.RemoveServer(id); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			future := &applyFuture{cmd: &Command{Type: CmdCreateNode}, errCh: make(chan error, 1)}
			r.processApply(ctx, future)
			r.sendHeartbeatsToAllPeers(ctx, 1)
		}
	}()
	wg.Wait()

	// startElection makes this node a candidate, so it runs after the
	// membership changes above, which need a leader.
	r.config.Raft.ElectionTimeout = time.Millisecond
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			r.peerMu.Lock()
			r.config.Raft.Peers = append([]PeerConfig(nil), r.config.Raft.Peers...)
			r.peerMu.Unlock()
		}
	}()
	for i := 0; i < 20; i++ {
		r.startElection(ctx)
	}
	<-done
}

// AddVoter and the commit path take logMu and peerMu together. They must take
// them in the same order (logMu, then peerMu), or they deadlock.
func TestRaftReplicator_AddVoterAndCommitDoNotDeadlock(t *testing.T) {
	r := newLeaderWithPeer(t)
	r.logMu.Lock()
	for i := 1; i <= 500; i++ {
		r.log = append(r.log, &RaftLogEntry{Index: uint64(i), Term: 1})
	}
	r.logMu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				r.advanceCommitIndex()
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				_ = r.AddVoter(fmt.Sprintf("v%d", i), "127.0.0.1:17203")
			}
		}()
		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("AddVoter and advanceCommitIndex deadlocked on logMu/peerMu")
	}
}

func newLockingHAReplicator(t *testing.T, role string, logger *slog.Logger) *HAStandbyReplicator {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Mode = ModeHAStandby
	cfg.NodeID = "ha-lock"
	cfg.AdvertiseAddr = "127.0.0.1:17211"
	cfg.HAStandby.Role = role
	cfg.HAStandby.PeerAddr = "127.0.0.1:17212"
	cfg.HAStandby.SyncMode = SyncAsync
	cfg.HAStandby.AutoFailover = false
	cfg.Logger = logger
	r, err := NewHAStandbyReplicator(cfg, NewMockStorage())
	if err != nil {
		t.Fatal(err)
	}
	r.SetTransport(NewMockTransport())
	return r
}

// HandleFence changes the role while the heartbeat paths read it.
func TestHAStandbyReplicator_FenceChangesRoleUnderMu(t *testing.T) {
	r := newLockingHAReplicator(t, "primary", nil)
	r.mu.Lock()
	r.standbyConn = &MockPeerConn{connected: true}
	r.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			if _, err := r.HandleFence(&FenceRequest{Reason: "test"}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			r.sendHeartbeat(context.Background())
			if _, err := r.HandleHeartbeat(&HeartbeatRequest{NodeID: "peer"}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	wg.Wait()
	if got := r.Health().Role; got != "fenced" {
		t.Fatalf("role after fence = %q, want fenced", got)
	}
}

// A manual Promote and the auto-failover goroutine can promote at the same
// time; only one of them may start the primary loops.
func TestHAStandbyReplicator_ConcurrentPromotePromotesOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 50; i++ {
		var logs bytes.Buffer
		r := newLockingHAReplicator(t, "standby", slog.New(slog.NewTextHandler(&logs, nil)))

		start := make(chan struct{})
		var wg sync.WaitGroup
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if err := r.Promote(ctx); err != nil {
					t.Error(err)
				}
			}()
		}
		close(start)
		wg.Wait()
		if err := r.Shutdown(); err != nil {
			t.Fatal(err)
		}

		if n := strings.Count(logs.String(), `msg="[HA] Promoted to primary"`); n != 1 {
			t.Fatalf("iteration %d: promoted %d times, want 1", i, n)
		}
	}
}

type promoteResult struct {
	err     error
	primary bool // IsLeader when Promote returned
}

// startHeldPromotes starts two Promotes while the WAL flush is held, so the
// first is still promoting when the second arrives. It fails the test if
// either returns before release is called.
func startHeldPromotes(t *testing.T, r *HAStandbyReplicator) (release func() []promoteResult) {
	t.Helper()
	r.walApplier.mu.Lock() // the first Promote blocks in Flush
	results := make(chan promoteResult, 2)
	promote := func() {
		err := r.Promote(context.Background())
		results <- promoteResult{err: err, primary: r.IsLeader()}
	}
	go promote()
	time.Sleep(50 * time.Millisecond)
	go promote()

	select {
	case res := <-results:
		r.walApplier.mu.Unlock()
		t.Fatalf("Promote returned (err=%v, primary=%v) while the first promotion was still flushing", res.err, res.primary)
	case <-time.After(200 * time.Millisecond):
	}
	return func() []promoteResult {
		r.walApplier.mu.Unlock()
		return []promoteResult{<-results, <-results}
	}
}

// A Promote that loses to a concurrent one must not report success before
// the winner has flushed the WAL and published primary status.
func TestHAStandbyReplicator_ConcurrentPromoteWaitsForPromotion(t *testing.T) {
	r := newLockingHAReplicator(t, "standby", nil)
	for _, res := range startHeldPromotes(t, r)() {
		if res.err != nil || !res.primary {
			t.Fatalf("Promote returned err=%v, primary=%v; want nil after primary is published", res.err, res.primary)
		}
	}
	if err := r.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

// When the promotion fails, every waiting caller gets its error, and the next
// call retries.
func TestHAStandbyReplicator_ConcurrentPromoteSharesFailure(t *testing.T) {
	r := newLockingHAReplicator(t, "standby", nil)
	store := r.storage.(*MockStorage)
	flushErr := errors.New("flush failed")
	store.mu.Lock()
	store.applyErr = flushErr
	store.mu.Unlock()
	r.walApplier.pendingBatch = []*WALEntry{{Position: 1, Command: &Command{Type: CmdCreateNode}}}

	for _, res := range startHeldPromotes(t, r)() {
		if !errors.Is(res.err, flushErr) || res.primary {
			t.Fatalf("Promote returned err=%v, primary=%v; want the flush error and still standby", res.err, res.primary)
		}
	}

	store.mu.Lock()
	store.applyErr = nil
	store.mu.Unlock()
	if err := r.Promote(context.Background()); err != nil || !r.IsLeader() {
		t.Fatalf("retry: err=%v, primary=%v; want nil and primary", err, r.IsLeader())
	}
	if err := r.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

// Shutdown may be called from several places at once; it must close stopCh
// exactly once.
func TestReplicators_ConcurrentShutdownClosesOnce(t *testing.T) {
	newReplicators := func(t *testing.T) []Replicator {
		raft, _ := newCoverageRaftReplicator(t)

		mrCfg := DefaultConfig()
		mrCfg.Mode = ModeMultiRegion
		mrCfg.NodeID = "mr-lock"
		mrCfg.MultiRegion.RegionID = "region-a"
		mrCfg.MultiRegion.LocalCluster = mrCfg.Raft
		mrCfg.MultiRegion.LocalCluster.Bootstrap = true
		mr, err := NewMultiRegionReplicator(mrCfg, NewMockStorage())
		if err != nil {
			t.Fatal(err)
		}

		return []Replicator{raft, newLockingHAReplicator(t, "standby", nil), mr}
	}

	for i := 0; i < 100; i++ {
		for _, r := range newReplicators(t) {
			start := make(chan struct{})
			var wg sync.WaitGroup
			for g := 0; g < 4; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					if err := r.Shutdown(); err != nil {
						t.Error(err)
					}
				}()
			}
			close(start)
			wg.Wait()
		}
	}
}
