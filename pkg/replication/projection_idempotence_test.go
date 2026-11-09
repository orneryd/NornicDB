// Author: NOR-291 implementation lane (Codex), 2026-10-27; TJ API rebase 2026-10-29.
// Adapted from the NOR-288 lane's test-only d663 projection control.
package replication

// Retry, recovery and streaming regressions for the accepted-command projection
// owner. Raft recovery qualification remains dependent on #1691.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNOR291RaftPendingProjection(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	fixture := setupTestAdapter(t, adapterFixtureOptions{backing: base})
	r, _ := newCoverageRaftReplicator(t)
	r.storage = fixture.adapter
	r.started.Store(true)
	r.mu.Lock()
	r.state, r.currentTerm = StateLeader, 1
	r.mu.Unlock()
	t.Cleanup(func() { _ = r.Shutdown() })
	failure := errors.New("native publication unavailable")
	var reject atomic.Bool
	reject.Store(true)
	require.NoError(t, base.InitializeStoredEntityObserver("test", func(storage.StoredEntitySnapshot) (storage.StoredEntityObserver, error) {
		return func(storage.StoredEntityPublication) (func(), error) {
			if reject.Load() {
				return nil, failure
			}
			return func() {}, nil
		}, nil
	}, nil))
	tx, err := base.BeginTransaction(context.Background())
	require.NoError(t, err)
	_, err = tx.CreateNode(context.Background(), &storage.Node{ID: "test:accepted"})
	require.NoError(t, err)
	p, err := tx.PrepareEntityCommit(context.Background(), storage.EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	t.Cleanup(p.Abort)
	receipt, err := NewReplicatedEngine(base, r, time.Second).SubmitEntityCommit(context.Background(), p, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case future := <-r.applyCh:
		r.processApply(ctx, future)
	case <-ctx.Done():
		t.Fatal("command did not reach Raft")
	}
	require.Equal(t, storage.CommitRecoveryPending, receipt.WaitOutcome(ctx).State)
	count := func() int {
		require.NoError(t, fixture.adapter.FlushWAL())
		entries, err := fixture.adapter.GetWALEntries(0, 100)
		require.NoError(t, err)
		n := 0
		for _, entry := range entries {
			if entry.Command.Type == CmdCommitTransaction && entry.Command.RequestID == p.LogicalID() {
				n++
			}
		}
		return n
	}
	require.Equal(t, 1, count(), "one first application projection")
	// Revisit while the native error remains permanent, without recovery.
	r.advanceCommitIndex()
	require.Zero(t, r.lastApplied)
	require.Equal(t, 1, count(), "same accepted ID must have one journal projection")
	// NOR-288 owns native recovery and the terminal join. Revisit the same
	// committed entry after that owner settles it; projection identity must
	// neither grow the journal nor keep lastApplied behind a recovered write.
	payload, err := storage.EncodeEntityTransaction(storage.EntityTransactionRequest{Prepared: p})
	require.NoError(t, err)
	reject.Store(false)
	require.NoError(t, storage.ApplyAcceptedEntityTransaction(base, payload))
	require.NoError(t, receipt.Wait(ctx))
	r.advanceCommitIndex()
	require.EqualValues(t, 1, r.lastApplied)
	require.Equal(t, 1, count(), "recovered application retains the original projection")
	_, err = base.GetStoredNode(context.Background(), "test:accepted")
	require.NoError(t, err)
}

// The native application deliberately fails: projection completion is independent
// of application success, and retries must continue reporting that failure.
func TestNOR291ProjectionIdentityAcrossPaths(t *testing.T) {
	for _, synchronous := range []bool{false, true} {
		for _, requestID := range []string{"accepted-command", ""} {
			t.Run(fmt.Sprintf("sync=%v/requestID=%v", synchronous, requestID != ""), func(t *testing.T) {
				fixture := setupTestAdapter(t)
				a := fixture.adapter
				mode := WALQueued
				if synchronous {
					mode = WALPublished
				}
				cmd := wireCommand(t, "accepted-command", storage.Operation{Type: storage.OpCreateEdge, Edge: wireEdge("edge", "missing-a", "missing-b", "value")})
				cmd.RequestID = requestID
				for range 3 {
					require.Error(t, applyNOR291(a, cmd, mode), "a projection must not hide native application failure")
				}
				assertNOR291ProjectionCount(t, a, 1)
				a.PruneWALEntries(100)
				require.Error(t, applyNOR291(a, cmd, mode))
				assertNOR291ProjectionCount(t, a, 1)
				require.NoError(t, a.Close())
				reopened, err := NewStorageAdapterWithWAL(a.engine, fixture.walDir)
				require.NoError(t, err)
				t.Cleanup(func() { _ = reopened.Close() })
				// Re-decode the serialized command, as replica and recovery paths do.
				data, err := storage.EncodeCommandPayload(cmd)
				require.NoError(t, err)
				var replay Command
				require.NoError(t, storage.DecodeCommandPayload(data, &replay))
				require.Error(t, applyNOR291(reopened, &replay, mode))
				assertNOR291ProjectionCount(t, reopened, 1)
				// Another accepted ID must retain its own projection.
				next := wireCommand(t, "another-command", storage.Operation{Type: storage.OpCreateEdge, Edge: wireEdge("edge", "missing-a", "missing-b", "value")})
				next.RequestID = "another-command"
				require.Error(t, applyNOR291(reopened, next, mode))
				assertNOR291ProjectionCount(t, reopened, 2)
			})
		}
	}
}

func TestNOR291ProjectionConcurrentAndReplicaRetry(t *testing.T) {
	mode := WALPublished
	a := setupTestAdapter(t).adapter
	cmd := wireCommand(t, "parallel", storage.Operation{Type: storage.OpCreateEdge, Edge: wireEdge("edge", "missing-a", "missing-b", "value")})
	cmd.RequestID = "parallel"
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() { require.Error(t, applyNOR291(a, cmd, mode)) })
	}
	wg.Wait()
	assertNOR291ProjectionCount(t, a, 1)
	applier := NewWALApplier(a)
	for range 3 {
		last, err := applier.ApplyBatch(t.Context(), []*WALEntry{{Position: 10, Command: cmd}})
		require.Error(t, err)
		require.Zero(t, last, "failed native application cannot acknowledge the replica position")
		require.Zero(t, applier.lastApplied)
	}
	assertNOR291ProjectionCount(t, a, 1)
}

// A retry must not add a projection, while a different accepted identity must.
func TestNOR291DistinctCommandAndRetryProjections(t *testing.T) {
	a := setupTestAdapter(t).adapter
	first := wireCommand(t, "first", storage.Operation{Type: storage.OpCreateEdge, Edge: wireEdge("edge-a", "missing-a", "missing-b", "value")})
	first.RequestID = "first"
	second := wireCommand(t, "second", storage.Operation{Type: storage.OpCreateEdge, Edge: wireEdge("edge-b", "missing-a", "missing-b", "value")})
	second.RequestID = "second"
	require.Error(t, applyNOR291(a, first, WALPublished))
	require.Error(t, applyNOR291(a, second, WALPublished))
	assertNOR291ProjectionCount(t, a, 2)
	require.Error(t, applyNOR291(a, first, WALPublished))
	assertNOR291ProjectionCount(t, a, 2)
}

func TestNOR291RetryReturnsOriginalPublicationPosition(t *testing.T) {
	a := setupTestAdapter(t).adapter
	payload, err := storage.EncodeCommandPayload(struct{ Prefix string }{Prefix: "unused:"})
	require.NoError(t, err)
	cmd := &Command{Type: CmdDeleteByPrefix, RequestID: "same-command", Data: payload, Timestamp: time.Now()}
	first, err := a.ApplyCommand(t.Context(), cmd, WALPublished)
	require.NoError(t, err)
	require.NotZero(t, first.Position)
	second, err := a.ApplyCommand(t.Context(), cmd, WALPublished)
	require.NoError(t, err)
	require.Equal(t, first.Position, second.Position)
	assertNOR291ProjectionCount(t, a, 1)
}

func TestNOR291ProjectionFailedAppendCanRetry(t *testing.T) {
	mode := WALPublished
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	a := setupTestAdapter(t, adapterFixtureOptions{backing: base}).adapter
	cmd := wireCommand(t, "append-retry", storage.Operation{Type: storage.OpCreateNode, Node: wireNode("n", "value")})
	cmd.RequestID = "append-retry"
	// Close and replace the field under its own lock: the asynchronous writer
	// reads a.wal while holding walMu, so the test must not touch it unlocked.
	a.walMu.Lock()
	closeErr := a.wal.Close()
	a.walMu.Unlock()
	require.NoError(t, closeErr)
	require.ErrorIs(t, applyNOR291(a, cmd, mode), storage.ErrWALClosed)
	// The barrier completes only after the writer left its batch, so no queued
	// request can still be appending through the closed WAL when it is replaced.
	require.ErrorIs(t, a.FlushWAL(), storage.ErrWALClosed)
	wal, err := storage.NewWAL(a.walDir, nil)
	require.NoError(t, err)
	a.walMu.Lock()
	a.wal = wal
	a.walMu.Unlock()
	require.NoError(t, applyNOR291(a, cmd, mode))
	assertNOR291PublishedProjectionCount(t, a, 1)
	node, err := storage.GetStoredNode(context.Background(), a.engine, "wire:n")
	require.NoError(t, err)
	require.Equal(t, "value", node.Properties["value"])
	require.NoError(t, applyNOR291(a, cmd, mode))
	assertNOR291PublishedProjectionCount(t, a, 1)
	// The worker retains its first append error for FlushWAL/Close even when
	// a subsequent published request succeeds; do not hide that failure.
	require.ErrorIs(t, a.FlushWAL(), storage.ErrWALClosed)
}

func TestNOR291ProjectionTailRepairRetainsIdentity(t *testing.T) {
	mode := WALPublished
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	dir := t.TempDir()
	a, err := NewStorageAdapterWithWAL(base, dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	cmd := wireCommand(t, "before-tail", storage.Operation{Type: storage.OpCreateNode, Node: wireNode("retained", "value")})
	cmd.RequestID = "before-tail"
	require.NoError(t, applyNOR291(a, cmd, mode))
	assertNOR291ProjectionCount(t, a, 1)
	require.NoError(t, a.Close())
	path := filepath.Join(dir, "wal.log")
	good, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Greater(t, len(good), 4)
	tail, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	require.NoError(t, err)
	// Copy only the start of an actual record emitted by the WAL owner.
	// This models an interrupted current-format append without a private codec.
	_, err = tail.Write(good[:4])
	require.NoError(t, err)
	require.NoError(t, tail.Close())
	reopened, err := NewStorageAdapterWithWAL(base, dir)
	require.NoError(t, err, "TJ's repair keeps the intact command prefix")
	t.Cleanup(func() { _ = reopened.Close() })
	require.NoError(t, applyNOR291(reopened, cmd, mode))
	assertNOR291ProjectionCount(t, reopened, 1)
}

func TestNOR291ProjectionUnreadableRecordDoesNotSkip(t *testing.T) {
	for _, record := range []any{map[string]any{"cmd": nil}, map[string]any{"cmd": "invalid"}} {
		mode := WALPublished
		base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
		fixture := setupTestAdapter(t, adapterFixtureOptions{backing: base})
		a := fixture.adapter
		// Use the existing JSON WAL owner to construct an unreadable command
		// record followed by a valid one, then force the persistent read path.
		require.NoError(t, a.wal.Append(storage.OperationType("replication_command"), record))
		cmd := wireCommand(t, "after-unreadable", storage.Operation{Type: storage.OpCreateNode, Node: wireNode("n", "value")})
		require.NoError(t, a.wal.Append(storage.OperationType("replication_command"), replicationWALRecord{Command: cmd}))
		require.NoError(t, a.wal.Sync())
		entries, err := a.GetWALEntries(0, 100)
		require.Error(t, err, "preserve TJ's refusal to acknowledge a window with an unreadable command")
		require.Empty(t, entries)
		require.NoError(t, a.Close())
		reopened, err := NewStorageAdapterWithWAL(a.engine, fixture.walDir)
		require.NoError(t, err)
		t.Cleanup(func() { _ = reopened.Close() })
		require.Error(t, applyNOR291(reopened, cmd, mode), "startup cannot discard unreadable projection identity")
		_, err = base.GetStoredNode(context.Background(), "wire:n")
		require.ErrorIs(t, err, storage.ErrNotFound, "unreadable identity history cannot mutate native state")
	}
}

func assertNOR291ProjectionCount(t *testing.T, a *StorageAdapter, count int) {
	t.Helper()
	require.NoError(t, a.FlushWAL())
	assertNOR291PublishedProjectionCount(t, a, count)
}

// Call only after a WALPublished request or a successful FlushWAL barrier.
func assertNOR291PublishedProjectionCount(t *testing.T, a *StorageAdapter, count int) {
	t.Helper()
	entries, err := a.GetWALEntries(0, 100)
	require.NoError(t, err)
	require.Len(t, entries, count, "streamed projection count")
	records, err := storage.ReadWALEntriesFromDir(t.Context(), a.walDir)
	require.NoError(t, err)
	var projections int
	for _, record := range records {
		if record.Operation == storage.OperationType("replication_command") {
			projections++
		}
	}
	require.Equal(t, count, projections, "physical projection count")
}

func applyNOR291(a *StorageAdapter, cmd *Command, mode WALPublicationMode) error {
	_, err := a.ApplyCommand(context.Background(), cmd, mode)
	return err
}
