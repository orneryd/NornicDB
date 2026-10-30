// Author: NOR-288 implementation lane (Codex), 2026-10-26.
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

func TestAcceptedForeignApplyWaitsForListenerDelivery(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	adapter := setupTestAdapter(t, adapterFixtureOptions{backing: base}).adapter
	entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var events atomic.Int32
	base.OnNodeCreated(func(*storage.Node) { events.Add(1); close(entered); <-release })
	payload, err := storage.EncodeEntityTransaction(storage.EntityTransactionRequest{Namespace: "test", LogicalID: "foreign-listener", Operations: []storage.Operation{{Type: storage.OpCreateNode, Node: &storage.Node{ID: "test:foreign"}}}})
	require.NoError(t, err)
	command := &Command{Type: CmdCommitTransaction, Data: payload, Timestamp: time.Now()}
	go func() { returned <- adapter.ApplyCommand(context.Background(), command) }()
	t.Cleanup(func() {
		unblock()
		select {
		case <-returned:
		case <-time.After(5 * time.Second):
			t.Error("foreign ApplyCommand listener did not join")
		}
	})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("native command did not reach its listener")
	}
	// Physical data is present, but public return must include listener work.
	_, err = base.GetStoredNode("test:foreign")
	require.NoError(t, err)
	select {
	case err := <-returned:
		returned <- err
		t.Fatal("foreign ApplyCommand returned before listener delivery completed")
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	select {
	case err := <-returned:
		returned <- err
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("foreign ApplyCommand did not return after listener completion")
	}
	require.EqualValues(t, 1, events.Load())
}

func TestAcceptedReplicaPendingApplicationRecovery(t *testing.T) {
	for _, transport := range []string{"standalone", "raft"} {
		t.Run(transport, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			t.Cleanup(func() { _ = base.Close() })
			fixture := setupTestAdapter(t, adapterFixtureOptions{backing: base, standalone: true, timeout: time.Second})
			engine := fixture.engine
			var raft *RaftReplicator
			if transport == "raft" {
				raft, _ = newCoverageRaftReplicator(t)
				raft.storage = fixture.adapter
				raft.started.Store(true)
				raft.mu.Lock()
				raft.state, raft.currentTerm = StateLeader, 1
				raft.mu.Unlock()
				t.Cleanup(func() { _ = raft.Shutdown() })
				engine = NewReplicatedEngine(base, raft, time.Second)
			}
			var reject atomic.Bool
			failure := errors.New("native publication unavailable")
			require.NoError(t, base.InitializeStoredEntityObserver("test", func(storage.StoredEntitySnapshot) (storage.StoredEntityObserver, error) {
				return func(storage.StoredEntityPublication) (func(), error) {
					if reject.Load() {
						return nil, failure
					}
					return func() {}, nil
				}, nil
			}, nil))
			tx, err := base.BeginTransaction()
			require.NoError(t, err)
			_, err = tx.CreateNode(&storage.Node{ID: "test:accepted", Properties: map[string]any{"v": "committed"}})
			require.NoError(t, err)
			p, err := tx.PrepareEntityCommit(context.Background(), storage.EntityCommitRequest{LogicalID: tx.ID})
			require.NoError(t, err)
			t.Cleanup(p.Abort)
			payload, err := storage.EncodeEntityTransaction(storage.EntityTransactionRequest{Prepared: p})
			require.NoError(t, err)
			// A peer is permitted to apply the durable command before the
			// leader's native attempt. It cannot roll this decision back.
			peer := storage.NewMemoryEngine()
			t.Cleanup(func() { _ = peer.Close() })
			require.NoError(t, storage.ApplyAcceptedEntityTransaction(peer, payload))
			reject.Store(true)
			var settled atomic.Int32
			receipt, err := engine.SubmitEntityCommit(context.Background(), p, func(storage.CommitOutcome) { settled.Add(1) })
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if raft != nil {
				select {
				case future := <-raft.applyCh:
					raft.processApply(ctx, future)
				case <-ctx.Done():
					t.Fatal("accepted command did not reach consensus queue")
				}
			}
			outcome := receipt.WaitOutcome(ctx)
			require.Equal(t, storage.CommitRecoveryPending, outcome.State)
			var accepted *storage.AcceptedCommitError
			require.ErrorAs(t, outcome.Err, &accepted)
			requireAcceptedDiagnosticCause(t, outcome.Err, failure)
			require.Zero(t, settled.Load())
			reject.Store(false)
			if raft == nil {
				require.NoError(t, storage.ApplyAcceptedEntityTransaction(base, payload))
			} else {
				// The existing ordered replay driver itself must resume the
				// accepted native owner after the obstruction is repaired.
				raft.advanceCommitIndex()
			}
			require.NoError(t, receipt.Wait(ctx), "replication must join the recovered native terminal")
			require.EqualValues(t, 1, settled.Load())
			if raft != nil {
				raft.advanceCommitIndex()
				require.EqualValues(t, 1, raft.lastApplied, "replay must retire the formerly pending committed entry")
			}
			want, err := peer.GetStoredNode("test:accepted")
			require.NoError(t, err)
			got, err := base.GetStoredNode("test:accepted")
			require.NoError(t, err)
			require.Equal(t, want.Properties, got.Properties)
			require.NoError(t, storage.ApplyAcceptedEntityTransaction(peer, payload))
			count, err := peer.NodeCount()
			require.NoError(t, err)
			require.EqualValues(t, 1, count)
			require.NoError(t, base.UpdateNode(storage.NodeUpdateRequest{After: &storage.Node{ID: "test:accepted", Properties: map[string]any{"v": "writable"}}}))
		})
	}
}

// Each accepted owner preserves its own diagnostic boundary. Traverse these
// explicitly; ordinary errors.Is must not treat any one of them as rejection.
// Author: NOR-288 lane (Codex), 2026-10-29.
func requireAcceptedDiagnosticCause(t *testing.T, err, target error) {
	t.Helper()
	require.NotErrorIs(t, err, target)
	seen := map[*storage.AcceptedCommitError]bool{}
	for {
		var accepted *storage.AcceptedCommitError
		if !errors.As(err, &accepted) {
			break
		}
		require.False(t, seen[accepted], "accepted diagnostic chain cannot cycle")
		seen[accepted] = true
		err = accepted.Cause()
	}
	require.NotEmpty(t, seen)
	require.ErrorIs(t, err, target)
}
