package storage

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestPR1655ObservedWALRejectsBeforeJournaling(t *testing.T) {
	for _, kind := range []string{"node", "edge"} {
		t.Run(kind, func(t *testing.T) {
			t.Cleanup(config.WithWALEnabled())
			b := newTestEngine(t)
			wal, err := NewWAL(t.TempDir(), nil)
			require.NoError(t, err)
			owner := NewWALEngine(b, wal)
			t.Cleanup(func() { require.NoError(t, owner.Close()) })
			var updateErr error
			if kind == "node" {
				updateErr = owner.UpdateNode(NodeUpdateRequest{Before: &Node{ID: "test:gone"}, After: &Node{ID: "test:gone", Properties: map[string]any{"v": "rejected"}}})
			} else {
				updateErr = owner.UpdateEdge(EdgeUpdateRequest{Before: &Edge{ID: "test:gone"}, After: &Edge{ID: "test:gone", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}})
			}
			require.ErrorIs(t, updateErr, ErrConflict)
			require.Zero(t, wal.Sequence(), "a rejected observation must not enter the replay log")
			entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
			require.NoError(t, err)
			require.Empty(t, entries)
			_, err = b.GetNode("test:gone")
			require.ErrorIs(t, err, ErrNotFound)
			_, err = b.GetEdge("test:gone")
			require.ErrorIs(t, err, ErrNotFound)
		})
	}
}

func TestPR1655ObservedWALAcceptedSemanticReplay(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	b := newTestEngine(t)
	destination := newTestEngine(t)
	wal, err := NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	owner := NewWALEngine(b, wal)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	for _, engine := range []*BadgerEngine{b, destination} {
		for _, id := range []NodeID{"test:a", "test:b"} {
			_, err := engine.CreateNode(&Node{ID: id, Properties: map[string]any{"v": "before"}})
			require.NoError(t, err)
		}
		require.NoError(t, engine.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"v": "before"}}))
	}
	beforeNode, err := b.GetNode("test:a")
	require.NoError(t, err)
	afterNode := *beforeNode
	afterNode.Properties = map[string]any{"v": "after"}
	require.NoError(t, owner.UpdateNode(NodeUpdateRequest{Before: beforeNode, After: &afterNode}))
	beforeEdge, err := b.GetEdge("test:r")
	require.NoError(t, err)
	afterEdge := *beforeEdge
	afterEdge.Properties = map[string]any{"v": "after"}
	require.NoError(t, owner.UpdateEdge(EdgeUpdateRequest{Before: beforeEdge, After: &afterEdge}))
	entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	for _, entry := range entries {
		require.Equal(t, OpEntityCommit, entry.Operation, "only accepted semantic images belong in recovery")
		require.NoError(t, ReplayWALEntry(destination, entry))
	}
	for _, engine := range []*BadgerEngine{b, destination} {
		node, err := engine.GetNode("test:a")
		require.NoError(t, err)
		require.Equal(t, "after", node.Properties["v"])
		edge, err := engine.GetEdge("test:r")
		require.NoError(t, err)
		require.Equal(t, "after", edge.Properties["v"])
	}
	require.Equal(t, "before", beforeNode.Properties["v"])
	require.Equal(t, "before", beforeEdge.Properties["v"])
}

type pr1655RejectedPhysicalSubmitter struct {
	Engine
	failure error
}

func (e *pr1655RejectedPhysicalSubmitter) GetInnerEngine() Engine { return e.Engine }
func (e *pr1655RejectedPhysicalSubmitter) SubmitEntityCommit(context.Context, *PreparedEntityCommit, func(CommitOutcome)) (*CommitReceipt, error) {
	return nil, e.failure
}

func TestPR1655ObservedWALPreservesAcceptedFailure(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	b := newTestEngine(t)
	_, err := b.CreateNode(&Node{ID: "test:a", Properties: map[string]any{"v": "before"}})
	require.NoError(t, err)
	before, err := b.GetNode("test:a")
	require.NoError(t, err)
	wal, err := NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	physicalErr := errors.New("physical submit unavailable after journal acceptance")
	owner := NewWALEngine(&pr1655RejectedPhysicalSubmitter{Engine: b, failure: physicalErr}, wal)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	err = owner.UpdateNode(NodeUpdateRequest{Before: before, After: &Node{ID: before.ID, Properties: map[string]any{"v": "accepted"}}})
	var accepted *AcceptedCommitError
	require.ErrorAs(t, err, &accepted)
	require.ErrorIs(t, accepted, physicalErr)
	require.Equal(t, CommitRecoveryPending, accepted.State)
	require.NotEmpty(t, accepted.LogicalID)
	_, pending := owner.pendingEntityCommits.Load(accepted.LogicalID)
	require.True(t, pending, "recovery pending ownership survives caller error")
	entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, OpEntityCommit, entries[0].Operation)
	stored, err := b.GetNode("test:a")
	require.NoError(t, err)
	require.Equal(t, "before", stored.Properties["v"], "physical rejection has not applied the accepted body")
}

func TestPR1655ObservedWALHonorsManagedAdmission(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	b := newTestEngine(t)
	_, err := b.CreateNode(&Node{ID: "test:a", Properties: map[string]any{"v": "before"}})
	require.NoError(t, err)
	before, err := b.GetNode("test:a")
	require.NoError(t, err)
	rejected := errors.New("managed coordinator rejected update")
	var calls int
	b.RegisterManagedEntityPreparer("test", func(ctx context.Context, operations []Operation, mode EntityCommitMode) (*PreparedEntityCommit, func(CommitOutcome), error) {
		calls++
		require.Equal(t, CommitModeDirect, mode)
		require.Len(t, operations, 1)
		require.Same(t, before, operations[0].NodeUpdate().Before)
		return nil, nil, rejected
	}, nil)
	wal, err := NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	owner := NewWALEngine(b, wal)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	err = owner.UpdateNode(NodeUpdateRequest{Before: before, After: &Node{ID: before.ID, Properties: map[string]any{"v": "rejected"}}})
	require.ErrorIs(t, err, rejected)
	require.Equal(t, 1, calls)
	require.Zero(t, wal.Sequence())
	stored, err := b.GetNode(before.ID)
	require.NoError(t, err)
	require.Equal(t, "before", stored.Properties["v"])
}

// A queued compaction writer must not slip between observation validation and
// journal admission, nor deadlock a recursive read-lock acquisition.
func TestPR1655ObservedWALPreparationHoldsSnapshotBarrier(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	b := newTestEngine(t)
	_, err := b.CreateNode(&Node{ID: "test:a", Properties: map[string]any{"v": "before"}})
	require.NoError(t, err)
	before, err := b.GetNode("test:a")
	require.NoError(t, err)
	wal, err := NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	owner := NewWALEngine(b, wal)
	owner.snapshotDir = t.TempDir()
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	updateDone, snapshotDone := make(chan error, 1), make(chan error, 1)
	t.Cleanup(func() {
		unblock()
		select {
		case <-updateDone:
		case <-time.After(5 * time.Second):
			t.Error("update worker did not join")
		}
		select {
		case <-snapshotDone:
		case <-time.After(5 * time.Second):
			t.Error("snapshot worker did not join")
		}
	})
	b.RegisterManagedEntityPreparer("test", func(ctx context.Context, operations []Operation, mode EntityCommitMode) (*PreparedEntityCommit, func(CommitOutcome), error) {
		tx, err := BeginEntityTransaction(b, mode)
		if err != nil {
			return nil, nil, err
		}
		defer tx.Rollback()
		if err = tx.SetNamespace("test"); err != nil {
			return nil, nil, err
		}
		if err = ApplyNodeUpdate(tx, operations[0].NodeUpdate()); err != nil {
			return nil, nil, err
		}
		prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: tx.ID})
		if err != nil {
			return nil, nil, err
		}
		close(entered)
		<-release
		return prepared, nil, nil
	}, nil)
	go func() {
		updateDone <- owner.UpdateNode(NodeUpdateRequest{Before: before, After: &Node{ID: before.ID, Properties: map[string]any{"v": "after"}}})
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("preparation did not enter")
	}
	unprotected := owner.mutationMu.TryLock()
	if unprotected {
		owner.mutationMu.Unlock()
	}
	go func() { snapshotDone <- owner.createSnapshotAndCompact() }()
	require.False(t, unprotected, "snapshot barrier must cover preparation before journal registration")
	deadline := time.Now().Add(5 * time.Second)
	for owner.mutationMu.TryRLock() {
		owner.mutationMu.RUnlock()
		if time.Now().After(deadline) {
			t.Fatal("compaction did not queue its exclusive mutation barrier")
		}
		runtime.Gosched()
	}
	unblock()
	select {
	case err := <-updateDone:
		updateDone <- err
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("update deadlocked behind queued compaction writer")
	}
	select {
	case err := <-snapshotDone:
		snapshotDone <- err
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("compaction did not finish after accepted update")
	}
	stored, err := b.GetNode(before.ID)
	require.NoError(t, err)
	require.Equal(t, "after", stored.Properties["v"])
	require.EqualValues(t, 1, owner.totalSnapshots.Load())
}

func TestPR1655ObservedWALUpdateKeepsMissingTimeDefaults(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	engine := newTestEngine(t)
	created := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	_, err := engine.CreateNode(&Node{ID: "test:a", CreatedAt: created, UpdatedAt: created})
	require.NoError(t, err)
	before, err := engine.GetNode("test:a")
	require.NoError(t, err)
	wal, err := NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	owner := NewWALEngine(engine, wal)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	require.NoError(t, owner.UpdateNode(NodeUpdateRequest{Before: before, After: &Node{ID: before.ID}}))
	stored, err := engine.GetNode(before.ID)
	require.NoError(t, err)
	require.True(t, created.Equal(stored.CreatedAt))
	require.False(t, stored.UpdatedAt.IsZero())
	entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	var payload walEntityCommitData
	require.NoError(t, decodeRecoveryPayload(entries[0].Data, &payload))
	_, ops, err := DecodeEntityTransaction(payload.Payload)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	require.True(t, stored.CreatedAt.Equal(ops[0].Node.CreatedAt))
	require.True(t, stored.UpdatedAt.Equal(ops[0].Node.UpdatedAt))
}
