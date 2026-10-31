// Author: NOR-288 implementation lane (Codex), 2026-10-27.
package storage

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAcceptedDuplicateReplayReleasesBothPreparedAuthorities(t *testing.T) {
	base := NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	_, err := base.CreateNode(&Node{ID: "test:n", Properties: map[string]any{"v": "before"}})
	require.NoError(t, err)
	blocker, err := base.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, blocker.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: "test:n", Properties: map[string]any{"v": "blocker"}}, Policy: ClientNodeEdits}))
	held, err := blocker.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: blocker.ID})
	require.NoError(t, err)
	entered, release := make(chan struct{}), make(chan struct{})
	var gate sync.Once
	var events atomic.Int32
	base.OnNodeUpdated(func(*Node) { events.Add(1); gate.Do(func() { close(entered); <-release }) })
	var unblock sync.Once
	var workers sync.WaitGroup
	t.Cleanup(func() { held.Abort(); unblock.Do(func() { close(release) }); workers.Wait() })
	waiting := make(chan struct{}, 8)
	restore := SetUniqueKeyLockWaitHookForTesting(func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	})
	t.Cleanup(restore)
	payload, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: "test", LogicalID: "duplicate-replay", Operations: []Operation{{Type: OpUpdateNode, Node: &Node{ID: "test:n", Properties: map[string]any{"v": "accepted"}}}}})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type submission struct {
		receipt *CommitReceipt
		err     error
	}
	results := make(chan submission, 2)
	for i := 0; i < 2; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			replayEnvelope, replayOperations, err := DecodeEntityTransaction(payload)
			require.NoError(t, err)
			r, e := SubmitAcceptedEntityTransaction(ctx, base, replayEnvelope, replayOperations, nil)
			results <- submission{r, e}
		}()
	}
	// Both calls passed registry discovery and staged before either native
	// owner can register. They wait on this actual prepared entity authority.
	for i := 0; i < 2; i++ {
		select {
		case <-waiting:
		case <-ctx.Done():
			t.Fatal("both accepted replicas did not stage before registration")
		}
	}
	held.Abort()
	receipts := make([]*CommitReceipt, 0, 2)
	for i := 0; i < 2; i++ {
		select {
		case result := <-results:
			require.NoError(t, result.err)
			receipts = append(receipts, result.receipt)
		case <-ctx.Done():
			t.Fatal("duplicate replicas did not join the accepted owner")
		}
	}
	require.Same(t, receipts[0], receipts[1], "one accepted logical ID owns one receipt")
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("accepted native application did not publish")
	}
	unblock.Do(func() { close(release) })
	require.NoError(t, receipts[0].Wait(ctx))
	require.NoError(t, receipts[1].Wait(ctx))
	require.EqualValues(t, 1, events.Load(), "duplicate preparation never publishes another entity image")
	// The losing prepared frame also retained authority. A fresh public
	// transaction must acquire it after the shared terminal outcome.
	next, err := base.BeginTransaction()
	require.NoError(t, err)
	defer next.Rollback()
	require.NoError(t, next.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: "test:n", Properties: map[string]any{"v": "writable"}}, Policy: ClientNodeEdits}))
	writeCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	prepared, err := next.PrepareEntityCommit(writeCtx, EntityCommitRequest{LogicalID: next.ID})
	require.NoError(t, err, "settlement must release every prepared hold of the same accepted command")
	require.NoError(t, prepared.Commit(nil))
	node, err := base.GetStoredNode("test:n")
	require.NoError(t, err)
	require.Equal(t, "writable", node.Properties["v"])
}
