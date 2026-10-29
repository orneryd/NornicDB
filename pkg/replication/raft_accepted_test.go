package replication

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type raftDeferredStorage struct {
	Storage
	fail    atomic.Bool
	failure error
}

func (s *raftDeferredStorage) ApplyCommand(cmd *Command) error {
	if s.fail.Load() {
		return s.failure
	}
	return s.Storage.ApplyCommand(cmd)
}

func TestRaftAcceptedEntityCommitOwnership(t *testing.T) {
	for _, route := range []string{"enqueue-shutdown", "raw-leader", "late", "role-change", "queued-shutdown", "commit-timeout", "apply-failure", "committed-shutdown", "uncommitted-truncate"} {
		t.Run(route, func(t *testing.T) {
			engine, adapter := wireFixture(t, false)
			r, _ := newCoverageRaftReplicator(t)
			r.storage = adapter
			r.started.Store(true)
			r.mu.Lock()
			r.state = StateLeader
			r.currentTerm = 1
			r.mu.Unlock()
			r.config.Raft.CommitTimeout = 20 * time.Millisecond
			t.Cleanup(func() { require.NoError(t, r.Shutdown()) })
			fault := &raftDeferredStorage{Storage: adapter, failure: errors.New("state machine unavailable")}
			if route == "apply-failure" || route == "committed-shutdown" {
				fault.fail.Store(true)
				r.storage = fault
			}
			if route == "commit-timeout" || route == "uncommitted-truncate" {
				r.peerMu.Lock()
				r.config.Raft.Peers = []PeerConfig{{ID: "peer"}}
				r.peerMu.Unlock()
			}
			if route == "raw-leader" {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				result := make(chan error, 1)
				go func() {
					_, err := NewReplicatedEngine(engine, r, time.Second).CreateNode(&storage.Node{ID: "wire:raw", Properties: map[string]any{"value": "leader-prepared"}})
					result <- err
				}()
				select {
				case future := <-r.applyCh:
					require.Equal(t, CmdCommitTransaction, future.cmd.Type)
					require.NotNil(t, future.accepted)
					r.processApply(ctx, future)
				case <-ctx.Done():
					t.Fatal("direct leader write was not prepared and queued")
				}
				select {
				case err := <-result:
					require.NoError(t, err)
				case <-ctx.Done():
					t.Fatal("direct leader write did not finish")
				}
				node, err := engine.GetStoredNode("wire:raw")
				require.NoError(t, err)
				require.Equal(t, "leader-prepared", node.Properties["value"])
				require.EqualValues(t, 1, r.lastApplied)
				return
			}
			tx, err := engine.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, tx.SetNamespace("wire"))
			_, err = tx.CreateNode(&storage.Node{ID: "wire:accepted", Properties: map[string]any{"value": "once"}})
			require.NoError(t, err)
			prepared, err := tx.PrepareEntityCommit(context.Background(), storage.EntityCommitRequest{LogicalID: tx.ID})
			require.NoError(t, err)
			if route == "enqueue-shutdown" {
				defer prepared.Abort()
				for len(r.applyCh) < cap(r.applyCh) {
					r.applyCh <- &applyFuture{cmd: &Command{Type: CmdUnknown}, errCh: make(chan error, 1)}
				}
				queueCtx := &raftQueueContext{Context: context.Background(), entered: make(chan struct{}), release: make(chan struct{})}
				t.Cleanup(func() { queueCtx.once.Do(func() { close(queueCtx.release) }) })
				queued := make(chan error, 1)
				go func() {
					_, err := r.SubmitAcceptedCommand(queueCtx, &Command{Type: CmdCommitTransaction, accepted: &acceptedEntityCommand{prepared: storage.NewAcceptedEntityCommitHandle(prepared)}}, nil)
					queued <- err
				}()
				<-queueCtx.entered
				stopped := make(chan struct{})
				go func() { _ = r.Shutdown(); close(stopped) }()
				<-r.stopCh
				select {
				case <-stopped:
					t.Fatal("shutdown drained before its admitted queue producer finished")
				case <-time.After(20 * time.Millisecond):
				}
				queueCtx.once.Do(func() { close(queueCtx.release) })
				require.ErrorIs(t, <-queued, ErrClosed)
				<-stopped
				require.Empty(t, r.applyCh)
				prepared.Abort()
				return
			}
			replicated := NewReplicatedEngine(engine, r, time.Second)
			var settled atomic.Int32
			receipt, err := replicated.SubmitEntityCommit(context.Background(), prepared, func(storage.CommitOutcome) { settled.Add(1) })
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if route == "queued-shutdown" {
				require.NoError(t, r.Shutdown())
			} else {
				var future *applyFuture
				select {
				case future = <-r.applyCh:
				case <-ctx.Done():
					t.Fatal("accepted command was not queued")
				}
				if route == "late" {
					canceled, stop := context.WithCancel(context.Background())
					stop()
					outcome := receipt.WaitOutcome(canceled)
					require.Equal(t, storage.CommitRecoveryPending, outcome.State)
					require.ErrorIs(t, outcome.Err, context.Canceled)
					require.Zero(t, settled.Load())
					_, err = engine.GetStoredNode("wire:accepted")
					require.ErrorIs(t, err, storage.ErrNotFound)
					require.ErrorIs(t, tx.Rollback(), storage.ErrTransactionClosed, "client cleanup cannot abort accepted staging")
				}
				if route == "role-change" {
					r.mu.Lock()
					r.state = StateFollower
					r.currentTerm = 2
					r.mu.Unlock()
				}
				r.processApply(context.Background(), future)
				switch route {
				case "commit-timeout":
					outcome := receipt.WaitOutcome(ctx)
					require.Equal(t, storage.CommitRecoveryPending, outcome.State)
					require.ErrorContains(t, outcome.Err, "commit timeout")
					require.Zero(t, settled.Load())
					_, err = engine.GetStoredNode("wire:accepted")
					require.ErrorIs(t, err, storage.ErrNotFound)
					r.peerMu.Lock()
					r.matchIndex["peer"] = 1
					r.peerMu.Unlock()
					r.advanceCommitIndex()
				case "apply-failure", "committed-shutdown":
					outcome := receipt.WaitOutcome(ctx)
					require.Equal(t, storage.CommitRecoveryPending, outcome.State)
					require.ErrorIs(t, outcome.Err, fault.failure)
					require.Zero(t, settled.Load())
					require.EqualValues(t, 1, r.commitIndex)
					require.Zero(t, r.lastApplied)
					if route == "committed-shutdown" {
						require.NoError(t, r.Shutdown())
						outcome = receipt.WaitOutcome(ctx)
						require.Equal(t, storage.CommitRecoveryPending, outcome.State)
						require.ErrorIs(t, outcome.Err, ErrClosed)
						require.Zero(t, settled.Load(), "shutdown cannot certify a committed entry discarded")
						select {
						case <-receipt.Done():
							t.Fatal("recovery-pending receipt became terminal")
						default:
						}
						require.NoError(t, engine.Close(), "pending logical ownership must not leak its native snapshot/barrier")
						return
					}
					fault.fail.Store(false)
					r.advanceCommitIndex()
				case "uncommitted-truncate":
					response := r.handleAppendEntriesRequest(&AppendEntriesRequest{Term: 2, LeaderID: "replacement", Entries: []*RaftLogEntry{{Index: 1, Term: 2, Command: &Command{Type: CmdUnknown}}}})
					require.True(t, response.Success)
				}
			}
			select {
			case <-receipt.Done():
			case <-ctx.Done():
				t.Fatal("accepted command did not reach its actual terminal outcome")
			}
			outcome := receipt.WaitOutcome(ctx)
			require.Equal(t, outcome.State, receipt.WaitPhysicalOutcome(ctx).State, "late physical completion must replace a prior pending error")
			require.EqualValues(t, 1, settled.Load())
			if route == "role-change" || route == "queued-shutdown" || route == "uncommitted-truncate" {
				require.Equal(t, storage.CommitDiscarded, outcome.State)
				if route == "queued-shutdown" {
					require.ErrorIs(t, outcome.Err, ErrClosed)
				} else {
					require.ErrorIs(t, outcome.Err, ErrNotLeader)
				}
				_, err = engine.GetStoredNode("wire:accepted")
				require.ErrorIs(t, err, storage.ErrNotFound)
				// Definite discard releases both native staging and accepted authority.
				_, err = engine.CreateNode(&storage.Node{ID: "wire:accepted"})
				require.NoError(t, err)
			} else {
				require.Equal(t, storage.CommitApplied, outcome.State)
				require.NoError(t, outcome.Err)
				node, err := engine.GetStoredNode("wire:accepted")
				require.NoError(t, err)
				require.Equal(t, "once", node.Properties["value"])
				count, err := engine.NodeCount()
				require.NoError(t, err)
				require.EqualValues(t, 1, count)
				require.EqualValues(t, 1, r.lastApplied)
			}
		})
	}
}

// Done pauses the producer at select evaluation, after admission, so shutdown
// cannot finish its drain until the in-flight producer has made its decision.
type raftQueueContext struct {
	context.Context
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *raftQueueContext) Done() <-chan struct{} {
	close(c.entered)
	<-c.release
	return nil
}
