package replication

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRaftCommittedApplyFailureRetainsApplicationFrontier(t *testing.T) {
	for _, route := range []string{"leader", "follower", "single"} {
		t.Run(route, func(t *testing.T) {
			r, store := newCoverageRaftReplicator(t)
			r.started.Store(true)
			r.mu.Lock()
			r.currentTerm = 1
			r.state = StateLeader
			r.mu.Unlock()
			first := &Command{Type: CmdCreateNode, Data: []byte("first")}
			second := &Command{Type: CmdCreateNode, Data: []byte("second")}
			fail := errors.New("storage apply failed")
			store.SetApplyError(fail)
			one := &applyFuture{cmd: first, index: 1, errCh: make(chan error, 1)}
			two := &applyFuture{cmd: second, index: 2, errCh: make(chan error, 1)}
			maxIndex := uint64(2)
			entries := []*RaftLogEntry{{Index: 1, Term: 1, Command: first}, {Index: 2, Term: 1, Command: second}}
			retry := func() { r.advanceCommitIndex() }
			switch route {
			case "single":
				maxIndex = 1
				r.processApply(context.Background(), one)
			case "leader":
				r.peerMu.Lock()
				r.config.Raft.Peers = []PeerConfig{{ID: "peer"}}
				r.matchIndex["peer"] = 2
				r.peerMu.Unlock()
				r.logMu.Lock()
				r.log = append(r.log, entries...)
				r.logMu.Unlock()
				r.futuresMu.Lock()
				r.futures[1] = one
				r.futures[2] = two
				r.futuresMu.Unlock()
				r.advanceCommitIndex()
			case "follower":
				request := &AppendEntriesRequest{Term: 1, LeaderID: "leader", PrevLogIndex: 0, PrevLogTerm: 0, Entries: entries, LeaderCommit: 2}
				retry = func() {
					response := r.handleAppendEntriesRequest(request)
					require.True(t, response.Success, "the log remains replicated even when application needs recovery")
				}
				retry()
			}
			r.logMu.RLock()
			committed, applied := r.commitIndex, r.lastApplied
			r.logMu.RUnlock()
			require.Equal(t, maxIndex, committed, "storage failure does not uncommit a consensus decision")
			require.Zero(t, applied, "a failed entry must remain the first unapplied entry")
			require.Zero(t, store.GetApplyCount(), "later committed entries must not overtake the failed entry")
			health := r.Health()
			require.False(t, health.Healthy)
			require.Equal(t, "apply_failed", health.State)
			require.Equal(t, committed, health.CommitIndex)
			require.Zero(t, health.AppliedIndex)
			if route != "follower" {
				select {
				case err := <-one.errCh:
					require.ErrorIs(t, err, fail, "failed storage application cannot acknowledge success")
				default:
					t.Fatal("failed application did not report its error")
				}
			}
			store.SetApplyError(nil)
			retry()
			r.logMu.RLock()
			applied = r.lastApplied
			r.logMu.RUnlock()
			require.Equal(t, maxIndex, applied)
			health = r.Health()
			require.True(t, health.Healthy)
			require.Equal(t, "ready", health.State)
			require.Equal(t, maxIndex, health.AppliedIndex)
			require.EqualValues(t, maxIndex, store.GetApplyCount())
			store.mu.RLock()
			commands := append([]*Command(nil), store.commands...)
			store.mu.RUnlock()
			require.Equal(t, "first", string(commands[0].Data))
			if maxIndex == 2 {
				require.Equal(t, "second", string(commands[1].Data))
			}
			if route == "leader" {
				select {
				case err := <-two.errCh:
					require.NoError(t, err)
				default:
					t.Fatal("later entry did not acknowledge successful recovery")
				}
			}
			retry()
			require.EqualValues(t, maxIndex, store.GetApplyCount(), "retry must not reapply successful entries")
		})
	}
}

// A wait deadline can fill the future before the committed entry reaches
// storage. Its old response must not park the committed application owner.
func TestRaftCommittedEntryOutlivesCompletedWaiter(t *testing.T) {
	r, store := newCoverageRaftReplicator(t)
	r.mu.Lock()
	r.state = StateLeader
	r.currentTerm = 1
	r.mu.Unlock()
	cmd := &Command{Type: CmdCreateNode, Data: []byte("accepted")}
	future := &applyFuture{index: 1, cmd: cmd, errCh: make(chan error, 1)}
	future.errCh <- context.DeadlineExceeded
	r.futures[1] = future
	r.log = append(r.log, &RaftLogEntry{Index: 1, Term: 1, Command: cmd})
	done := make(chan struct{})
	go func() { defer close(done); r.advanceCommitIndex() }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		<-future.errCh // release a broken blocking sender before failing
		<-done
		t.Fatal("completed waiter blocked committed application")
	}
	require.EqualValues(t, 1, r.lastApplied)
	require.Equal(t, 1, store.GetApplyCount())
	require.ErrorIs(t, <-future.errCh, context.DeadlineExceeded)
	require.Empty(t, r.futures)
}

type raftHealthCallbackStorage struct {
	*MockStorage
	inspect func()
}

func (s *raftHealthCallbackStorage) ApplyCommand(cmd *Command) error {
	s.inspect()
	return s.MockStorage.ApplyCommand(cmd)
}

func TestRaftApplicationCallbackCanInspectHealth(t *testing.T) {
	for _, route := range []string{"single", "leader", "follower"} {
		t.Run(route, func(t *testing.T) {
			r, store := newCoverageRaftReplicator(t)
			r.mu.Lock()
			r.state = StateLeader
			r.currentTerm = 1
			r.mu.Unlock()
			observed := make(chan *HealthStatus, 1)
			release := make(chan struct{})
			r.storage = &raftHealthCallbackStorage{MockStorage: store, inspect: func() {
				checked := make(chan struct{})
				go func() { defer close(checked); observed <- r.Health() }()
				select {
				case <-checked:
				case <-release:
				}
			}}
			cmd := &Command{Type: CmdCreateNode, Data: []byte("health")}
			done := make(chan struct{})
			go func() {
				defer close(done)
				switch route {
				case "single":
					r.processApply(context.Background(), &applyFuture{cmd: cmd, errCh: make(chan error, 1)})
				case "leader":
					r.peerMu.Lock()
					r.config.Raft.Peers = []PeerConfig{{ID: "peer"}}
					r.matchIndex["peer"] = 1
					r.peerMu.Unlock()
					r.logMu.Lock()
					r.log = append(r.log, &RaftLogEntry{Index: 1, Term: 1, Command: cmd})
					r.logMu.Unlock()
					r.advanceCommitIndex()
				case "follower":
					r.handleAppendEntriesRequest(&AppendEntriesRequest{Term: 1, LeaderID: "leader", Entries: []*RaftLogEntry{{Index: 1, Term: 1, Command: cmd}}, LeaderCommit: 1})
				}
			}()
			select {
			case health := <-observed:
				require.EqualValues(t, 0, health.AppliedIndex, "callback runs before current application returns")
			case <-time.After(3 * time.Second):
				close(release)
				<-done
				<-observed
				t.Fatal("storage callback could not inspect Raft health")
			}
			close(release)
			<-done
			require.Equal(t, 1, store.GetApplyCount())
		})
	}
}
